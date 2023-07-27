<?php

namespace Sue\Async\Mysql;

use Closure;
use InvalidArgumentException;
use BadMethodCallException;
use React\Promise\PromiseInterface;
use React\Promise\Promise;
use Psr\Log\LoggerInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;
use Illuminate\Database\Query\Grammars\MySqlGrammar;
use Illuminate\Database\Connection as BaseConnection;
use Illuminate\Database\Query\Expression;
use Illuminate\Database\QueryException;
use Sue\Async\Mysql\Query\QueryManager;
use Sue\Async\Mysql\Query\QueryResult;
use Sue\Async\Mysql\Query\QueryBuilder;
use Sue\Async\Mysql\MysqlProcessor;
use Sue\Async\Mysql\Query\QueryOption;
use Sue\Async\Mysql\Query\Transaction;

use function React\Promise\reject;

class Connection extends BaseConnection
{
    use LoggerAwareTrait;

    /** @var QueryManager $queryManager */
    protected $queryManager;

    /** @var MySqlGrammar $queryGrammar */
    protected $queryGrammar;

    /** @var MysqlProcessor $postProcessor */
    protected $postProcessor;

    /** @var bool $loggingQueries 是否记录SQL */
    protected $loggingQueries = false;

    /** @var array $queryLog 执行的SQL日志 */
    protected $queryLog = [];

    /** @var string $databaseName */
    protected $databaseName;

    /** @var float $defaultTimeout 默认超时时间*/
    protected $defaultTimeout = 30;

    public function __construct(array $config, LoggerInterface $logger = null)
    {
        if (!extension_loaded('mysqli')) {
            throw new BadMethodCallException("mysqli extension is not loaded");
        }

        $this->setLogger($logger ?: new NullLogger());
        
        $this->databaseName = $config['database'];
        $this->queryGrammar = new MySqlGrammar();
        $this->postProcessor = new MysqlProcessor();
        $this->defaultTimeout = OptionConst::fetch($config, OptionConst::MAX_RUNNING_SECONDS);
        $this->queryManager = new QueryManager($config, $this->logger);
    }

    /**
     * 设置pipe回收debounce延迟
     *
     * @param float $seconds
     * @return static
     */
    public function setPipeRecycleDebounceSeconds($seconds)
    {
        $seconds = (float) $seconds;
        $pipe_manager = $this->queryManager->getPipeManager();
        $pipe_manager->setPipeRecycleDebounceSeconds($seconds);
        return $this;
    }

    /**
     * 设置pipe ping debounce延迟
     *
     * @param float $seconds
     * @return static
     */
    public function setPipePingDebounceSeconds($seconds)
    {
        $seconds = (float) $seconds;
        $pipe_manager = $this->queryManager->getPipeManager();
        $pipe_manager->setPipePingDebounceSeconds($seconds);
        return $this;
    }

    /** 
     * @inheritDoc
     * @return QueryBuilder
     */
    public function table($table, $as = null)
    {
        return $this->query()->from($table, $as);
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return Promise<\stdClass>
     */
    public function selectOne($sql, $bindings = [], $option = null)
    {
        return $this->execute($sql, $bindings, $option)
            ->then(function (QueryResult $result) {
                return $result->fetchOne();
            });
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return Promise<\stdClass[]>
     */
    public function select($sql, $bindings = [], $option = null)
    {
        return $this->execute($sql, $bindings, $option)
            ->then(function (QueryResult $result) {
                return $result->fetchAll();
            });
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return Promise<\Generator>
     */
    public function cursor($sql, $bindings = [], $option = null)
    {
        return $this->execute($sql, $bindings, $option)
            ->then(function (QueryResult $result) {
                return $result->generator();
            });
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return PromiseInterface|Promise
     */
    public function insert($sql, $bindings = [], QueryOption $option = null)
    {
        return $this->statement($sql, $bindings, $option);
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return PromiseInterface|Promise
     */
    public function insertGetId($sql, $bindings = [], QueryOption $option = null)
    {
        return $this->execute($sql, $bindings, $option)
            ->then(function (QueryResult $result) {
                return $result->getLastInsertId();
            });
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return PromiseInterface|Promise
     */
    public function update($sql, $bindings = [], QueryOption $option = null)
    {
        return $this->affectingStatement($sql, $bindings, $option);
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return PromiseInterface|Promise
     */
    public function delete($sql, $bindings = [], QueryOption $option = null)
    {
        return $this->affectingStatement($sql, $bindings, $option);
    }

    /** @inheritDoc */
    public function raw($value)
    {
        return new Expression($value);
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return PromiseInterface|Promise
     */
    public function unprepared($sql, QueryOption $option = null)
    {
        return $this->statement($sql, [], $option);
    }

    /** 
     * @inheritDoc
     * @throws BadMethodCallException
     */
    public function pretend(Closure $callback)
    {
        throw new BadMethodCallException();
    }

    /** @inheritDoc */
    public function disconnect()
    {
        $this->queryManager->close();
        $this->logger->debug("connection {$this->getName()} disconnected");
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return PromiseInterface|Promise
     */
    public function statement($sql, $bindings = [], QueryOption $option = null)
    {
        return $this->execute($sql, $bindings, $option)
            ->then(function () {
                return true;
            });
    }

    /** 
     * @inheritDoc 
     * @param QueryOption $option
     * @return PromiseInterface|Promise
     */
    public function affectingStatement($sql, $bindings = [], QueryOption $option = null)
    {
        return $this->execute($sql, $bindings, $option)
            ->then(function (QueryResult $result) {
                return $result->getNumRowAffected();
            });
    }

    /** @inheritDoc */
    public function getDatabaseName()
    {
        return $this->databaseName;
    }

    /** 
     * @inheritDoc
     * @param  float $timeout 超时时间（秒)，默认180秒
     * @throws BadMethodCallException
     */
    public function beginTransaction($timeout = 180)
    {
        $timeout = (float) $timeout;
        $option = $this->initDefaultOption()->setUseWrite(true);
        return $this->execute(Transaction::STMT_START, [], $option)
            ->then(function (QueryResult $result) use ($timeout) {
                $pipe = $result->pipe();
                $transaction = new Transaction($pipe, $timeout);
                $pipe->setTransaction($transaction);
                $this->fireConnectionEvent('beganTransaction');
                return $transaction;
            });
    }

    /**
     * 关闭数据库事务
     *
     * @param Transaction $transaction
     * @return void
     */
    public function closeTransaction(Transaction $transaction)
    {
        $pipe = $transaction->pipe() and $pipe->close();
    }

    /** 
     * @inheritDoc
     */
    public function commit(Transaction $transaction = null)
    {
        if ($rejected = $this->assertTransaction($transaction)) {
            return $rejected;
        }

        $option = $this->initDefaultOption()->setTransaction($transaction);
        return $this->execute(Transaction::STMT_COMMIT, [], $option)
            ->then(function () {
                $this->fireConnectionEvent('committed');
                return true;
            }, function ($error) use ($transaction) {
                $this->logger->error("error while transaction commit. \n {$error}");
                $transaction->close();
                throw $error;
            });
    }

    /** 
     * @inheritDoc
     */
    public function rollBack($transaction = null)
    {
        if ($rejected = $this->assertTransaction($transaction)) {
            return $rejected;
        }

        $option = $this->initDefaultOption()->setTransaction($transaction);
        return $this->execute(Transaction::STMT_ROLLBACK, [], $option)
            ->then(function () {
                $this->fireConnectionEvent('rollingBack');
                return true;
            }, function ($error) use ($transaction) {
                $this->logger->error("error while transaction rollback. \n {$error}");
                $transaction->close();
                throw $error;
            });
    }

    /** 
     * @inheritDoc
     * @throws BadMethodCallException
     */
    public function transactionLevel()
    {
        throw new BadMethodCallException();
    }

    /** 
     * @inheritDoc
     */
    public function transaction(Closure $callback, $attempts = 1)
    {
        throw new BadMethodCallException();
    }

    /**
     * 执行SQL
     *
     * @param string $sql
     * @param array $binds
     * @param QueryOption $option
     * @return PromiseInterface|Promise
     */
    protected function execute($sql, array $bindings = [], QueryOption $option = null)
    {
        $sql = (string) $sql;

        foreach ($this->beforeExecutingCallbacks as $beforeExecutingCallback) {
            $beforeExecutingCallback($sql, $bindings, $this);
        }

        $option = $option ?: $this->initDefaultOption();
        if (
            $option->transaction
            and $rejected = $this->assertTransaction($option->transaction)
        ) {
            return $rejected;
        }

        $start = microtime(true);
        /** @var Promise $promise */
        return $this->queryManager
            ->execute($sql, $bindings, $option)
            ->otherwise(function ($e) use ($sql, $bindings) {
                return reject(new QueryException($sql, $bindings, $e));
            })
            ->always(function () use ($sql, $bindings, $start) {
                $this->logQuery($sql, $bindings, $this->getElapsedTime($start));
            });
    }

    /** @inheritDoc */
    public function query()
    {
        return (new QueryBuilder($this, $this->queryGrammar, $this->postProcessor))
            ->setOption($this->initDefaultOption());
    }

    /**
     * 生成默认option信息
     * 
     * @return QueryOption
     */
    private function initDefaultOption()
    {
        $option = new QueryOption();
        $option->setTimeout($this->defaultTimeout);
        return $option;
    }

    /**
     * 判断是否是有效Transaction
     *
     * @param mixed $transaction
     * @return void
     * @throws InvalidArgumentException
     */
    private function assertTransaction($transaction)
    {
        $template = "Expecting an instance of Sue\\Async\\Mysql\\Query\\Transaction, but: ";
        $message = '';
        if (!is_object($transaction)) {
            $message = $template . gettype($transaction);
        } elseif (!($transaction instanceof Transaction)) {
            $message = $template . get_class($transaction);
        } elseif (null === $transaction->pipe()) {
            $message = "Transaction is closed";
        }

        return $message
            ? reject(new InvalidArgumentException($message))
            : null;
    }
}
