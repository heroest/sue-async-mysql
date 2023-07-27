<?php

namespace Sue\Async\Mysql;

final class OptionConst
{
    /** SQL执行的默认最长执行时间，默认0，表示不限制 */
    const MAX_RUNNING_SECONDS = 'max_running_seconds';
    
    /** 数据库限制时间限制，默认15秒 */
    const IDLE_TIMEOUT = 'idle_timeout';

    /** 数据库Query waiting list大小，默认100个名额 */
    const WAITING_LIST_SIZE = 'waiting_list_size';

    /** 默认连接数量, 默认1个 */
    const NUM_CONNECTIONS = 'num_connections';
    
    /** 最少保留几个闲置的连接，默认1个 */
    const MIN_NUM_IDLE_CONNECTIONS = 'min_num_idle_connections';

    const DEFAULTS = [
        self::MAX_RUNNING_SECONDS => 0,
        self::IDLE_TIMEOUT => 30,
        self::WAITING_LIST_SIZE => 100,
        self::NUM_CONNECTIONS => 1,
        self::MIN_NUM_IDLE_CONNECTIONS => 1,
    ];

    public static function fetch(array $values, $key)
    {
        $key = (string) $key;

        return isset($values[$key])
            ? $values[$key] 
            : self::DEFAULTS[$key];
    }
}