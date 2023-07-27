<?php

namespace Sue\Async\Mysql\Query;

use InvalidArgumentException;
use Sue\Async\Mysql\Query\Transaction;

/**
 * @property float $timeout SQL超时时间， <= 0表示不限制
 * @property null|Transaction $transaction 事务对象, 默认null
 * @property bool $useWrite 是否强制读主，默认false
 */
class QueryOption
{
    /** @var float $timeout */
    private $timeout = 0;

    /** @var null|Transaction $transaction */
    private $transaction;

    /** @var bool $useWrite */
    private $useWrite = false;

    /**
     * 设置超时时间(秒)
     *
     * @param float $interval_seconds
     * @return static
     */
    public function setTimeout($interval_seconds)
    {
        $interval_seconds = (float) $interval_seconds;

        $this->timeout = $interval_seconds;
        return $this;
    }

    /**
     * 设置数据库事务
     *
     * @param Transaction|null $transaction
     * @return static
     */
    public function setTransaction(Transaction $transaction = null)
    {
        $this->transaction = $transaction;
        return $this;
    }

    /**
     * 设置是否强制读主
     *
     * @param bool $use_write
     * @return static
     */
    public function setUseWrite($use_write = true)
    {
        $use_write = (bool) $use_write;

        $this->useWrite = $use_write;
        return $this;
    }

    public function __get($name)
    {
        if (property_exists($this, $name)) {
            return $this->$name;
        } else {
            throw new InvalidArgumentException("Unknown property: {$name}");
        }
    }
}