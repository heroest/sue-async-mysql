<?php

namespace Sue\Async\Mysql;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\MySqlProcessor as BaseProcessor;
use Sue\Async\Mysql\Connection;
use Sue\Async\Mysql\Query\QueryOption;

class MysqlProcessor extends BaseProcessor
{
    /**
     * Undocumented function
     *
     * @param Builder $query
     * @param string $sql
     * @param array $values
     * @param QueryOption|null $option
     * @return int
     */
    public function processInsertGetId(
        Builder $query,
        $sql,
        $values,
        $option = null
    ) {
        /** @var Connection $connection */
        $connection = $query->getConnection();
        return $connection->insertGetId($sql, $values, $option);
    }
}
