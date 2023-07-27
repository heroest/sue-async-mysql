<?php

namespace Sue\Async\Mysql;

use SplObjectStorage;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use React\Promise\Promise;
use React\Promise\PromiseInterface;
use React\Promise\Deferred;
use React\EventLoop\TimerInterface;
use Illuminate\Support\Arr;
use Sue\Async\Mysql\Pipe;
use Sue\Async\Mysql\OptionConst;
use Sue\Async\Mysql\Query\Transaction;
use Sue\Async\Mysql\Exceptions\PipeException;

use function Sue\Coroutine\SystemCall\cancel;
use function Sue\EventLoop\cancelTimer;
use function Sue\EventLoop\debounceById;
use function Sue\EventLoop\setInterval;
use function React\Promise\resolve;
use function React\Promise\reject;

class PipeManager
{
    use LoggerAwareTrait;

    /** @var Pipe[] $write */
    private $write = [];

    /** @var Pipe[] $read */
    private $read = [];

    /** @var Deferred[] $awaits */
    private $awaits = [];

    /** @var null|TimerInterface awaitResolver */
    private $awaitResolver = null;

    /** @var int $waitingListSize */
    private $waitingListSize;

    /** @var float $idleTimeout */
    private $idleTimeout;

    /** @var int $minNumIdleConnections */
    private $minNumIdleConnections;

    /** @var int $index 自增id */
    private $index = 0;

    /** @var float $recycleDebounceSeconds 延迟收回pipe的时间（秒） */
    private $recycleDebounceSeconds = 0; //默认不开启

    /** @var float $pingDebounceSeconds 延迟ping时间（秒） */
    private $pingDebounceSeconds = 0; //默认不开启

    /** @var null|TimerInterface $timerRecyclePipes */
    private $timerRecyclePipes;

    /** @var null|TimerInterface $timerPingPipes */
    private $timerPingPipes;

    /** @var Promise[]|SplObjectStorage $pendings */
    private $pendings;


    public function __construct(array $config, LoggerInterface $logger)
    {
        $this->setLogger($logger);

        $this->waitingListSize = (int) OptionConst::fetch($config, OptionConst::WAITING_LIST_SIZE);
        $this->idleTimeout = (float) OptionConst::fetch($config, OptionConst::IDLE_TIMEOUT);
        $this->minNumIdleConnections = (int) OptionConst::fetch($config, OptionConst::MIN_NUM_IDLE_CONNECTIONS);

        $write = Arr::get($config, 'write', []);
        $read = Arr::get($config, 'read', []);
        unset($config['write'], $config['read']);
        $this->initPipes(array_merge($config, $write), 'write');
        $this->initPipes(array_merge($config, $read), 'read');

        $this->pendings = new SplObjectStorage();
    }

    /**
     * 设置pipe 回收的debounce时间
     *
     * @param float $seconds
     * @return void
     */
    public function setPipeRecycleDebounceSeconds($seconds)
    {
        $seconds = (float) $seconds;

        $this->recycleDebounceSeconds = $seconds;
        if ($this->timerRecyclePipes) {
            cancelTimer($this->timerRecyclePipes);
            $this->timerRecyclePipes = null;
        }

        if ($this->recycleDebounceSeconds > 0) {
            $this->timerRecyclePipes = setInterval(
                $this->idleTimeout,
                function () {
                    $this->debounceRecycleByRole('read');
                    $this->debounceRecycleByRole('write');
                }
            );
        }
    }

    /**
     * 设置pipe定时ping的debounce时间
     *
     * @param float $seconds
     * @return void
     */
    public function setPipePingDebounceSeconds($seconds)
    {
        $seconds = (float) $seconds;

        $this->pingDebounceSeconds = $seconds;
        if ($this->timerPingPipes) {
            cancelTimer($this->timerPingPipes);
            $this->timerPingPipes = null;
        }

        if ($this->pingDebounceSeconds > 0) {
            $this->timerPingPipes = setInterval(
                (float) bcmul($this->pingDebounceSeconds, 10, $seconds),
                function () {
                    $this->debouncePingByRole('read');
                    $this->debouncePingByRole('write');
                }
            );
        }
    }

    /**
     * 关闭所有管道，reject所有await promise
     *
     * @return void
     */
    public function close()
    {
        foreach ($this->write as $pipe) {
            $pipe->close();
        }

        foreach ($this->read as $pipe) {
            $pipe->close();
        }

        if ($this->awaitResolver) {
            cancelTimer($this->awaitResolver);
            $this->awaitResolver = null;
        }
        foreach ($this->awaits as $promise_id => $await) {
            /** @var Deferred $deferred */
            $deferred = $await[0];
            $deferred->reject(new PipeException('Pipe closed'));
            unset($this->awaits[$promise_id]);
        }

        foreach ($this->pendings as $promise) {
            /** @var Promise $promise */
            $this->pendings->detach($promise);
            $promise->cancel();
        }
        $this->logger->debug("pipe manager closed");
    }

    /**
     * 获取绑定事务的数据库连接
     *
     * @param Transaction $transaction
     * @return PromiseInterface|Promise
     */
    public function getByTransaction(Transaction $transaction)
    {
        if ($pipe = $this->doGetByTransaction($transaction)) {
            $this->debouncePingByRole(Pipe::ROLE_WRITE);
            return resolve($pipe);
        }

        return $this->await($transaction);
    }

    /**
     * 获取指定roleName的数据库连接
     *
     * @param string $role_name
     * @return PromiseInterface|Promise
     */
    public function get($role_name)
    {
        $role_name = (string) $role_name;

        if ($pipe = $this->doGet($role_name)) {
            return resolve($pipe);
        } elseif (!$this->isAwaitable()) {
            return reject(new PipeException('waiting list is full'));
        }

        $this->logger->debug("put on waiting list [{$role_name}]");
        return $this->await(null, $role_name);
    }

    /**
     * 解析待处理的连接
     *
     * @return void
     */
    public function resolveAwait()
    {
        $num_resolved = 0;
        foreach ($this->awaits as $promise_id => list($deferred, $transaction, $role_name)) {
            $pipe = $transaction
                ? $this->getByTransaction($transaction)
                : $this->doGet($role_name);
            if ($pipe) {
                $num_resolved++;
                $deferred->resolve($pipe);
                unset($this->awaits[$promise_id]);
            }
        }
        $num_resolved and $this->logger->debug("Resolved {$num_resolved} awaits");

        if (empty($this->awaits)) {
            cancelTimer($this->awaitResolver);
            $this->awaitResolver = null;
        }
    }

    /**
     * 解析待处理的连接
     * 
     * @param Transaction|null $transaction
     * @param string $role_name
     *
     * @return void
     */
    private function await(Transaction $transaction = null, $role_name = '')
    {
        $promise_id = null;
        $cleaner = function () use (&$promise_id) {
            unset($this->awaits[$promise_id]);
        };
        $deferred = new Deferred($cleaner);
        $promise = $deferred->promise();
        $promise_id = spl_object_hash($promise);
        $this->awaits[$promise_id] = [$deferred, $transaction, $role_name];
        if (null === $this->awaitResolver) {
            $this->awaitResolver = setInterval(0, [$this, 'resolveAwait']);
        }
        return $promise;
    }

    /**
     * 尝试获取一个链接
     *
     * @param string $role_name
     * @return Pipe|null
     */
    private function doGet($role_name)
    {
        /** @var Pipe[] $candidates */
        $candidates = [];
        foreach ($this->{$role_name} as $pipe) {
            /** @var Pipe $pipe */
            if ($pipe->transaction()) {
                continue;
            } elseif ($pipe->inState(Pipe::STATE_IDLE)) {
                $this->logger->debug("found a idle [{$role_name}] pipe:{$pipe->id()}");
                $this->debouncePingByRole($role_name);
                return $pipe;
            } elseif ($pipe->inState(Pipe::STATE_CLOSE)) {
                $candidates[] = $pipe;
            }
        }

        foreach ($candidates as $candidate) {
            if ($candidate->open()) {
                $this->logger->debug("opened a closed [{$role_name}] pipe:{$candidate->id()}");
                $this->debouncePingByRole($role_name);
                return $candidate;
            }
        }
        return null;
    }

    /**
     * 获取事务绑定的链接
     *
     * @param Transaction $transaction
     * @return Pipe|null
     */
    private function doGetByTransaction(Transaction $transaction)
    {
        $pipe = $transaction->pipe();
        return $pipe->inState(Pipe::STATE_IDLE)
            ? $pipe
            : null;
    }

    /**
     * 初始化链接
     *
     * @param array $config
     * @param string $role_name
     * @return void
     */
    private function initPipes(array $config, $role_name)
    {
        $this->logger->debug("initializing pipes for {$role_name}");
        $this->{$role_name} = [];
        $count_connections = OptionConst::fetch($config, OptionConst::NUM_CONNECTIONS);
        $config['host'] = (array) $config['host'];
        $count_host = count($config['host']);
        $credential = [
            'host' => '',
            'username' => $config['username'],
            'password' => $config['password'],
            'dbname' => $config['database'],
            'port' => $config['port'],
            'charset' => Arr::get($config, 'charset', 'utf8mb4'),
            'collation' => Arr::get($config, 'collation', 'utf8mb4_unicode_ci'),
        ];
        $indexes_to_open = range(0, $this->minNumIdleConnections - 1);
        foreach (range(0, $count_connections - 1) as $i) {
            $merged = array_merge($credential, ['host' => $config['host'][$i % $count_host]]);
            $pipe = new Pipe(++$this->index, $merged);
            $this->{$role_name}[] = $pipe;
            $pipe->setLogger($this->logger);
            in_array($i, $indexes_to_open) and $pipe->open();
        }
    }

    private function isAwaitable()
    {
        return count($this->awaits) < $this->waitingListSize;
    }

    public function debounceRecycleByRole($role_name)
    {
        if ($this->recycleDebounceSeconds <= 0) {
            return;
        }

        $key = __METHOD__ . ':' . $role_name;
        $promise = null;
        $that = $this;
        $promise = debounceById(
            $key,
            $this->recycleDebounceSeconds,
            function () use ($role_name) {
                $this->recycle($role_name);
            }
        )->always(static function () use (&$promise, &$that) {
            $that->pendings->detach($promise);
            $that = null;
        });
        $this->pendings->attach($promise);
    }

    public function debouncePingByRole($role_name)
    {
        if ($this->pingDebounceSeconds <= 0) {
            return;
        }

        $key = __METHOD__ . ':' . $role_name;
        $promise = null;
        $that = $this;
        $promise = debounceById(
            $key,
            $this->pingDebounceSeconds,
            function () use ($role_name) {
                $this->ping($role_name);
            }
        )->always(static function () use (&$promise, &$that) {
            $that->pendings->detach($promise);
        });
        $this->pendings->attach($promise);
    }

    /**
     * 尝试回收数据库连接
     *
     * @param string $role_name
     * @return void
     */
    private function recycle($role_name)
    {
        $this->logger->debug("start recycling for [{$role_name}] pipes");

        /** @var Pipe[] $pipes */
        $pipes = $this->{$role_name};
        $candidates = [];
        foreach ($pipes as $pipe) {
            if (
                $pipe->inState(Pipe::STATE_IDLE)
                and null === $pipe->transaction()
                and $pipe->getStateDuration() >= $this->idleTimeout
            ) {
                $candidates[] = [
                    'pipe' => $pipe,
                    'duration' => $pipe->getStateDuration()
                ];
            }
        }

        if (empty($candidates)) {
            $this->logger->debug("finished recycling for [{$role_name}] pipes, nothing to recycle");
            return;
        }

        $count_candidates = count($candidates);
        $count_closing = $count_candidates - $this->minNumIdleConnections;
        if ($count_closing <= 0) {
            $this->logger->debug("finished recycling for [{$role_name}] pipes, nothing to recycle");
            return;
        } elseif ($count_candidates > $count_closing) {
            usort($candidates, function ($a, $b) {
                return $b['duration'] - $a['duration'];
            });
            $chunks = array_chunk($candidates, $count_closing);
            $candidates = $chunks[0];
        }
        foreach ($candidates as $row) {
            $pipe = $row['pipe'];
            $pipe->close();
            $this->logger->debug("pipe:{$pipe->id()} pipe closed during recycling");
        }
    }

    private function ping(string $role_name)
    {
        /** @var Pipe[] $pipes */
        $pipes = $this->{$role_name};
        foreach ($pipes as $pipe) {
            if ($pipe->inState(Pipe::STATE_CLOSE)) {
                continue;
            }

            if ($pipe->ping()) {
                $this->logger->debug("pipe:{$pipe->id()} ping result is good");
                return;
            } elseif ($pipe->inState(Pipe::STATE_IDLE)) {
                $this->logger->warning("pipe:{$pipe->id()} pipe closed since ping failed");
                $pipe->close();
            } else {
                $this->logger->warning("pipe:{$pipe->id()} ping failed");
            }
        }
    }
}
