<?php

namespace Sue\Async\Mysql\Query;

use Exception;
use Throwable;
use SplObjectStorage;
use Psr\Log\LoggerInterface;
use Psr\Log\LoggerAwareTrait;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Promise\Promise;
use React\EventLoop\TimerInterface;
use Sue\Async\Mysql\Pipe;
use Sue\Async\Mysql\PipeManager;
use Sue\Async\Mysql\Exceptions\QueryException;
use Sue\Async\Mysql\Query\QueryOption;

use function Sue\EventLoop\cancelTimer;
use function Sue\EventLoop\loop;
use function Sue\EventLoop\setInterval;
use function React\Promise\Timer\timeout;

class QueryManager
{
    use LoggerAwareTrait;

    const SQL_ROLE_PATTERN = '/^\s*(SELECT|SHOW|DESCRIBE)\b/i';

    /** @var SplObjectStorage $working */
    private $working;

    /** @var PipeManager $pm */
    private $pm;

    /** @var null|TimerInterface $poll */
    private $poll;

    public function __construct(array $config, LoggerInterface $logger)
    {
        $this->setLogger($logger);
        $this->working = new SplObjectStorage();
        $this->pm = new PipeManager($config, $logger);
    }

    /**
     * 获取pipe-manager
     * 
     * @return PipeManager
     */
    public function getPipeManager()
    {
        return $this->pm;
    }

    /**
     * 执行SQL
     *
     * @param string $sql
     * @param array $bindings
     * @param QueryOption $option
     * @return PromiseInterface|Promise
     */
    public function execute($sql, array $bindings, QueryOption $option)
    {
        $sql = (string) $sql;

        /** @var Pipe|null $pipe */
        $pipe = null;
        if ($transaction = $option->transaction) {
            $connected = $this->pm->getByTransaction($transaction);
            $role_name = Pipe::ROLE_WRITE;
        } else {
            $role_name = $option->useWrite
                ? Pipe::ROLE_WRITE
                : $this->getPipeRole($sql);
            $this->logger->debug("fetching [{$role_name}] pipe for query", compact('sql', 'bindings'));
            $connected = $this->pm->get($role_name);
        }

        $deferred = new Deferred(function ($_, $reject) use (&$pipe, $connected) {
            $exception = new QueryException('[QueryManager] Connection closed due to promise cancelling');
            $reject($exception);
            $this->logger->debug("query manager promise cancelled", compact('sql', 'bindings'));
            if ($pipe) {
                $this->working->detach($pipe->getLink());
                $pipe->close();
            } else {
                $connected->cancel();
            }
        });

        /** @var Promise $promise */
        $promise = $deferred->promise();
        $promise = $promise->always(function () use (&$pipe, $role_name) {
            $pipe and $this->working->detach($pipe);
            $this->pm->debounceRecycleByRole($role_name);
        });

        /** @var Promise $connected */
        $connected->done(
            function (Pipe $opened) use ($sql, $bindings, $deferred, &$pipe) {
                $pipe = $opened;
                $this->logger->debug(
                    "got a pipe for query, pipe:{$pipe->id()}",
                    compact('sql', 'bindings')
                );
                $handler = function ($value) use ($deferred, $pipe, $sql, $bindings) {
                    $this->logger->debug(
                        "query result is settled, pipe:{$pipe->id()}",
                        compact('sql', 'bindings')
                    );
                    ($value instanceof Throwable or $value instanceof Exception)
                        ? $deferred->reject($value)
                        : $deferred->resolve($value);
                };
                $pipe->query($sql, $bindings)->done($handler, $handler);
                $this->working->attach($pipe->getLink(), $pipe);
                $this->polling();
            },
            function ($exception) use ($deferred) {
                $deferred->reject($exception);
            }
        );

        return $option->timeout > 0
            ? timeout($promise, $option->timeout, loop())
            : $promise;
    }

    /**
     * 关闭QueryManager
     *
     * @return void
     */
    public function close()
    {
        //先把工作中的连接都关闭
        $this->working->removeAll($this->working);
        if ($this->poll) {
            cancelTimer($this->poll);
            $this->poll = null;
        }
        //再把pm关闭
        $this->pm->close();
    }

    public function poll()
    {
        /** 
         * @var \mysqli[] $read 
         * @var \mysqli[] $error
         * @var \mysqli[] $reject
         */
        $read = $error = $reject = [];
        foreach ($this->working as $link) {
            /** @var \mysqli $link */
            $read[] = $error[] = $reject[] = $link;
        }

        if (!mysqli_poll($read, $error, $reject, 0)) {
            return;
        }

        foreach ($read as $link) {
            /** @var Pipe $pipe */
            $pipe = $this->working[$link];
            $this->working->detach($link);
            (false !== $result = $link->reap_async_query())
                ? $pipe->resolve($result)
                : $pipe->reject(new QueryException($link->error, $link->errno));
        }

        foreach ($error as $link) {
            /** @var Pipe $pipe */
            $pipe = $this->working[$link];
            $this->working->detach($link);
            $pipe->reject(new QueryException($link->error, $link->errno));
        }

        foreach ($reject as $link) {
            /** @var Pipe $pipe */
            $pipe = $this->working[$link];
            $this->working->detach($link);
            $pipe->reject(new QueryException($link->error, $link->errno));
        }

        if (0 === $this->working->count()) {
            cancelTimer($this->poll);
            $this->poll = null;
        }
    }

    private function polling()
    {
        if (null === $this->poll) {
            $this->poll = setInterval(0, [$this, 'poll']);
        }
    }

    private function getPipeRole(string $sql)
    {
        return preg_match(self::SQL_ROLE_PATTERN, $sql) > 0
            ? Pipe::ROLE_READ
            : Pipe::ROLE_WRITE;
    }
}
