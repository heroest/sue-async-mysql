<?php

namespace Sue\Async\Mysql\Query;

use Sue\Async\Mysql\Pipe;
use React\EventLoop\TimerInterface;

use function Sue\EventLoop\cancelTimer;
use function Sue\EventLoop\setTimeout;

class Transaction
{
    const STMT_START = 'START TRANSACTION';
    const STMT_COMMIT = 'COMMIT';
    const STMT_ROLLBACK = 'ROLLBACK';

    /** @var null|Pipe $pipe */
    private $pipe;

    /** @var null|TimerInterface $timer */
    private $timer;

    public function __construct(Pipe $pipe = null, $timeout)
    {
        $timeout = (float) $timeout;
        
        $this->pipe = $pipe;
        $this->timer = setTimeout($timeout, function () {
            $this->pipe and $this->pipe->close();
        });
    }

    /**
     * 获取数据库事务绑定的连接
     *
     * @return Pipe|null
     */
    public function pipe()
    {
        return $this->pipe;
    }

    /**
     * 关闭数据库事务
     * 
     * @return void
     */
    public function close()
    {
        cancelTimer($this->timer);
        $pipe = $this->pipe;
        $this->pipe = null;
        $pipe and $pipe->close();
    }
}
