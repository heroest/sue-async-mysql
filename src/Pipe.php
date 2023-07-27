<?php

namespace Sue\Async\Mysql;

use Throwable;
use Exception;
use mysqli;
use mysqli_result;
use Psr\Log\LoggerAwareTrait;
use React\Promise\Deferred;
use React\Promise\Promise;
use React\Promise\PromiseInterface;
use Illuminate\Support\Str;
use Sue\Async\Mysql\Exceptions\PipeException;
use Sue\Async\Mysql\Query\QueryResult;
use Sue\Async\Mysql\Query\Transaction;

use function React\Promise\reject;

class Pipe
{
    use LoggerAwareTrait;

    const STATE_CLOSE = -1;
    const STATE_IDLE = 0;
    const STATE_WORKING = 1;

    const ROLE_WRITE = 'write';
    const ROLE_READ = 'read';

    /** @var int $state */
    private $state = self::STATE_CLOSE;

    /** @var null|mysqli $link */
    private $link;

    private $credential = [];

    /** @var null|Deferred $deferred */
    private $deferred;

    /** @var null|Transaction $transaction */
    private $transaction;

    /** @var float|int|false $timeState */
    private $timeState = false;

    /** @var string $id */
    private $id = '';

    public function __construct($id, array $credential)
    {
        $this->id = (string) $id;
        $this->credential = $credential;
        $this->changeState(self::STATE_CLOSE);
    }

    public function id()
    {
        return $this->id;
    }

    /**
     * 获取mysqli连接对象
     *
     * @return mysqli|null
     */
    public function getLink()
    {
        return $this->link;
    }

    /**
     * 建立数据库连接
     *
     * @return boolean
     */
    public function open()
    {
        if (self::STATE_IDLE === $this->state) {
            return true;
        } elseif (self::STATE_CLOSE !== $this->state) {
            return false;
        }

        $credential = $this->credential;
        try {
            $this->link = new mysqli(
                $credential['host'],
                $credential['username'],
                $credential['password'],
                $credential['dbname'],
                $credential['port']
            );
            $sql = "SET NAMES {$credential['charset']} COLLATE {$credential['collation']}";
            $this->link->query($sql);
            $this->changeState(self::STATE_IDLE);
            return true;
        } catch (Throwable $e) {
            $this->logThrowable($e);
            return false;
        } catch (Exception $e) {
            $this->logThrowable($e);
            return false;
        }
    }

    /**
     * 关闭连接
     *
     * @return void
     */
    public function close()
    {
        if ($this->link) {
            $this->link->kill($this->link->thread_id);
            $this->link->close();
            $this->link = null;
        }

        if ($deferred = $this->deferred) {
            $this->deferred = null;
            $deferred->reject(new PipeException("pipe:{$this->id} is forced to close"));
        }

        if ($transaction = $this->transaction) {
            $this->transaction = null;
            $transaction->close();
        }

        $this->changeState(self::STATE_CLOSE);
        $this->logger->debug("pipe:{$this->id} closed");
    }

    /**
     * ping
     * 
     * @return bool
     */
    public function ping()
    {
        if (null === $this->link) {
            return false;
        } 
        
        try {
            return $this->link->ping();
        } catch (Throwable $e) {
            $this->logThrowable($e);
            return false;
        } catch (Exception $e) {
            $this->logThrowable($e);
            return false;
        }
    }

    /**
     * Undocumented function
     *
     * @param Transaction|null $transaction
     * @return self|static
     */
    public function setTransaction(Transaction $transaction = null)
    {
        $this->transaction = $transaction;
        return $this;
    }

    /**
     * 获取当前事务对象
     *
     * @return Transaction|null
     */
    public function transaction()
    {
        return $this->transaction;
    }

    /**
     * 查询
     *
     * @param string $sql
     * @param array $bindings
     * @return PromiseInterface|Promise
     */
    public function query($sql, array $bindings = [])
    {
        $sql = (string) $sql;

        if (self::STATE_IDLE !== $this->state) {
            return reject(new PipeException("pipe:{$this->id} is not ready for query"));
        }
        $bindings and $sql = $this->bindParamsToSql($sql, $bindings);
        $that = $this;
        $this->deferred = new Deferred(static function () use (&$that) {
            $that->logger->debug("pipe:{$that->id} close due to cancelling while querying");
            $that->close();
            $that = null;
        });
        $this->link->query($sql, MYSQLI_ASYNC);
        $this->changeState(self::STATE_WORKING);
        /** @var Promise $promise */
        $promise = $this->deferred->promise();
        return $promise->always(function () {
            $this->reset();
        });
    }

    /**
     * resolve
     *
     * @param mixed $mixed
     * @return void
     */
    public function resolve($mixed)
    {
        $this->deferred->resolve($this->fetchQueryResult($mixed));
    }

    /**
     * reject
     *
     * @param Throwable|Exception $e
     * @return void
     */
    public function reject($e)
    {
        $this->deferred->reject($e);
    }

    /**
     * 是否处于某状态
     *
     * @param integer $state
     * @return boolean
     */
    public function inState($state)
    {
        $state = (int) $state;

        return $state === $this->state;
    }

    /**
     * 获取当前状态持续时间
     *
     * @return void
     */
    public function getStateDuration()
    {
        return false === $this->timeState
            ? 0.00
            : (float) bcsub(microtime(true), $this->timeState, 4);
    }

    /**
     * 修改状态
     *
     * @param integer $state
     * @return boolean
     */
    private function changeState($state)
    {
        if ($this->state === $state) {
            return false;
        }

        $this->timeState = microtime(true);
        $this->state = $state;
        return true;
    }

    private function fetchQueryResult($mixed)
    {
        if ($mixed instanceof mysqli_result) {
            return QueryResult::useQueryResult($this, $mixed);
        } else {
            return QueryResult::useExecutionResult(
                $this,
                (string) $this->link->insert_id,
                $this->link->affected_rows
            );
        }
    }

    private function reset()
    {
        $this->deferred = null;
        $this->changeState(self::STATE_IDLE);
    }

    /**
     * 将参数绑定至SQL中
     *
     * @param string $sql
     * @param array $bindings
     * @return string
     */
    protected function bindParamsToSql($sql, array $bindings)
    {
        $sql = (string) $sql;

        $compiled = '';
        $rest = $sql;
        while (count($bindings)) {
            $value = array_shift($bindings);
            if (null === $value) {
                $value = 'null';
            } elseif (is_string($value)) {
                $value = sprintf("'%s'", $this->link->real_escape_string($value));
            }
            $head = Str::before($rest, '?');
            $compiled .= "{$head}{$value}";
            $rest = Str::after($rest, '?');
        }
        return $compiled . $rest;
    }

    protected function logThrowable($throwable)
    {
        /** @var Throwable|Exception $throwable */
        $class = get_class($throwable);
        $code = $throwable->getCode();
        $message = $throwable->getMessage();

        $content = "[{$class}:{$code}]: {$message}";
        $trace_stacks = explode("\n", $throwable->getTraceAsString());
        $this->logger->error($content, ['_trace_stacks' => $trace_stacks]);
    }
}
