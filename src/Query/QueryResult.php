<?php

namespace Sue\Async\Mysql\Query;

use stdClass;
use Exception;
use Throwable;
use Generator;
use mysqli_result;
use Sue\Async\Mysql\Exceptions\QueryException;
use Sue\Async\Mysql\Pipe;

class QueryResult
{
    /** @var mysqli_result $result */
    private $result;

    /** @var null|string $lastInsertId */
    private $lastInsertId;

    /** @var null|int $numRowAffected */
    private $numRowAffected;

    /** @var Pipe $pipe */
    private $pipe;

    private function __construct(
        Pipe $pipe,
        mysqli_result $result = null,
        $last_insert_id = '',
        $num_row_affected = 0
    ) {
        $this->pipe = $pipe;
        $this->result = $result;
        $this->lastInsertId = $last_insert_id;
        $this->numRowAffected = $num_row_affected;
    }

    /**
     * 构建一个mysql查询结果集
     *
     * @param Pipe $pipe
     * @param mysqli_result $result
     * @return self|static
     */
    public static function useQueryResult(Pipe $pipe, mysqli_result $result)
    {
        return new self($pipe, $result);
    }

    /**
     * 构建一个基于数据库变更记录的结果集
     *
     * @param Pipe $pipe
     * @param string $last_insert_id
     * @param integer $num_row_affected
     * @return self
     */
    public static function useExecutionResult(Pipe $pipe, $last_insert_id, $num_row_affected)
    {
        $last_insert_id = (string) $last_insert_id;
        $num_row_affected = (int) $num_row_affected;

        return new self(
            $pipe,
            null,
            $last_insert_id,
            $num_row_affected
        );
    }

    /**
     * 获取最后一次插入id
     *
     * @return string|null
     */
    public function getLastInsertId()
    {
        return $this->lastInsertId;
    }

    /**
     * 获取pipe
     *
     * @return Pipe
     */
    public function pipe()
    {
        return $this->pipe;
    }

    /**
     * 获取影响的行数
     * 
     * @return null|int
     */
    public function getNumRowAffected()
    {
        return $this->numRowAffected;
    }

    /**
     * 获取所有结果集
     *
     * @return array
     */
    public function fetchAll()
    {
        try {
            $data = [];
            while (null !== $row = $this->result->fetch_object()) {
                if (false === $row) {
                    throw new QueryException("error occurred while fetching data");
                }
                $data[] = $row;
            }
            return $data;
        } catch (Throwable $e) {
            throw $e;
        } catch (Exception $e) {
            throw $e;
        } finally {
            $this->result->close();
            $this->result = null;
        }
    }

    /**
     * 获取单个结果
     * 
     * @return null|stdClass
     */
    public function fetchOne()
    {
        try {
            if (false === $row = $this->result->fetch_object()) {
                throw new QueryException("error occurred while fetching data");
            }
            return $row;
        } catch (Throwable $e) {
            throw $e;
        } catch (Exception $e) {
            throw $e;
        } finally {
            $this->result->close();
            $this->result = null;
        }
    }

    /**
     * 获取返回集的迭代器
     *
     * @return Generator
     */
    public function generator()
    {
        try {
            while (null !== $row = $this->result->fetch_object()) {
                if (false === $row) {
                    throw new QueryException("error occurred while fetching data");
                }
                yield $row;
            }
        } catch (Throwable $e) {
            throw $e;
        } catch (Exception $e) {
            throw $e;
        } finally {
            $this->result->close();
            $this->result = null;
        }
    }
}
