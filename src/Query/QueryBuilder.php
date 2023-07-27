<?php

namespace Sue\Async\Mysql\Query;

use BadMethodCallException;
use Closure;
use Generator;
use React\Promise\Promise;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\RecordsNotFoundException;
use Illuminate\Database\MultipleRecordsFoundException;
use Illuminate\Support\LazyCollection;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Sue\Async\Mysql\Connection;
use Sue\Async\Mysql\MysqlProcessor;

use function React\Promise\resolve;
use function Sue\Coroutine\co;
use function Sue\Coroutine\SystemCall\returnValue;

class QueryBuilder extends Builder
{
    /** @var Connection $connection */
    public $connection;

    /** @var MysqlProcessor $processor */
    public $processor;

    /** @var null|QueryOption $option */
    private $option;

    /**
     * 设置Query参数
     *
     * @param QueryOption $option
     * @return static
     */
    public function setOption(QueryOption $option)
    {
        $this->option = $option;
        return $this;
    }

    /**
     * 获取Query参数
     *
     * @return QueryOption
     */
    public function getOption()
    {
        return $this->option;
    }

    /**
     * 设置超时时间
     *
     * @param float $timeout
     * @return static
     */
    public function setTimeout($timeout)
    {
        $timeout = (float) $timeout;

        $this->option->setTimeout($timeout);
        return $this;
    }

    /**
     * 设置事务处理
     *
     * @param Transaction $transaction
     * @return static
     */
    public function setTransaction(Transaction $transaction)
    {
        $this->option->setTransaction($transaction);
        return $this;
    }

    /**
     * 是否强制读主
     *
     * @param boolean $bool
     * @return static
     */
    public function setUseWrite($bool = true)
    {
        $bool = (bool) $bool;

        $this->option->setUseWrite($bool);
        return $this;
    }

    /**
     * @inheritDoc
     * @return Promise<\Illuminate\Support\LazyCollection>
     */
    public function cursor()
    {
        if (is_null($this->columns)) {
            $this->columns = ['*'];
        }

        $this->option->setUseWrite($this->useWritePdo);
        $sql = $this->toSql();
        $binding = $this->getBindings();
        return $this->connection
            ->cursor($sql, $binding, $this->option)
            ->then(function (Generator $generator) {
                return new LazyCollection($generator);
            });
    }

    /**
     * @inheritDoc
     * @return Promise<\Illuminate\Support\Collection>
     */
    public function get($columns = ['*'])
    {
        return $this->runSelect()
            ->then(function (array $results) use ($columns) {
                return collect(
                    $this->onceWithColumns(
                        Arr::wrap($columns),
                        function () use ($results) {
                            return $this->processor->processSelect($this, $results);
                        }
                    )
                );
            });
    }

    /**
     * @inheritDoc
     * @return Promise<bool>
     */
    public function insert(array $values)
    {
        // Since every insert gets treated like a batch insert, we will make sure the
        // bindings are structured in a way that is convenient when building these
        // inserts statements by verifying these elements are actually an array.
        if (empty($values)) {
            return resolve(true);
        }

        if (!is_array(reset($values))) {
            $values = [$values];
        }

        // Here, we will sort the insert keys for every record so that each insert is
        // in the same order for the record. We need to make sure this is the case
        // so there are not any errors or problems when inserting these records.
        else {
            foreach ($values as $key => $value) {
                ksort($value);

                $values[$key] = $value;
            }
        }

        $this->applyBeforeQueryCallbacks();

        // Finally, we will run this query against the database connection and return
        // the results. We will need to also flatten these bindings before running
        // the query so they are all in one huge, flattened array for execution.
        return $this->connection->insert(
            $this->grammar->compileInsert($this, $values),
            $this->cleanBindings(Arr::flatten($values, 1)),
            $this->option
        );
    }

    /**
     * @inheritDoc
     * @return Promise<int>
     */
    public function insertOrIgnore(array $values)
    {
        if (empty($values)) {
            return 0;
        }

        if (!is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }

        $this->applyBeforeQueryCallbacks();

        return $this->connection->affectingStatement(
            $this->grammar->compileInsertOrIgnore($this, $values),
            $this->cleanBindings(Arr::flatten($values, 1)),
            $this->option
        );
    }

    /**
     * @inheritDoc
     * @return Promise<int>
     */
    public function insertGetId(array $values, $sequence = null)
    {
        $this->applyBeforeQueryCallbacks();
        $sql = $this->grammar->compileInsertGetId($this, $values, $sequence);
        $values = $this->cleanBindings($values);
        return $this->processor->processInsertGetId(
            $this,
            $sql,
            $values,
            $this->option
        );
    }

    /**
     * @inheritDoc
     * @return Promise<int>
     */
    public function insertUsing(array $columns, $query)
    {
        $this->applyBeforeQueryCallbacks();

        list($sql, $bindings) = $this->createSub($query);

        return $this->connection->affectingStatement(
            $this->grammar->compileInsertUsing($this, $columns, $sql),
            $this->cleanBindings($bindings),
            $this->option
        );
    }

    /**
     * @inheritDoc
     * @return Promise<int>
     * 返回更新数量
     */
    public function update(array $values)
    {
        $this->applyBeforeQueryCallbacks();

        $sql = $this->grammar->compileUpdate($this, $values);

        $bindings = $this->cleanBindings(
            $this->grammar->prepareBindingsForUpdate($this->bindings, $values)
        );
        return $this->connection->update($sql, $bindings, $this->option);
    }

    /**
     * @inheritDoc
     * @return Promise<bool>
     */
    public function updateOrInsert(array $attributes, array $values = [])
    {
        return co(function (array $attributes, array $values) {
            if (false === yield $this->where($attributes)->exists()) {
                yield returnValue(
                    yield $this->insert(array_merge($attributes, $values))
                );
            }

            if (empty($values)) {
                // return true;
                yield returnValue(true);
            }

            yield returnValue(
                (bool) yield $this->limit(1)->update($values)
            );
        }, $attributes, $values);
    }

    /**
     * @inheritDoc
     * @return Promise<int>
     */
    public function upsert(array $values, $unique_by = '', $update = null)
    {
        return co(function (array $values, $unique_by, $update) {
            if (empty($values)) {
                // return 0;
                yield returnValue(0);
            } elseif ($update === []) {
                //return (int) yield $this->insert($values);
                yield returnValue((int) yield $this->insert($values));
            }
    
            if (! is_array(reset($values))) {
                $values = [$values];
            } else {
                foreach ($values as $key => $value) {
                    ksort($value);
    
                    $values[$key] = $value;
                }
            }
    
            if (is_null($update)) {
                $update = array_keys(reset($values));
            }
    
            $this->applyBeforeQueryCallbacks();
    
            $bindings = $this->cleanBindings(array_merge(
                Arr::flatten($values, 1),
                collect($update)->reject(function ($value, $key) {
                    return is_int($key);
                })->all()
            ));
    
            yield returnValue(
                $this->connection->affectingStatement(
                    $this->grammar->compileUpsert($this, $values, (array) $unique_by, $update),
                    $bindings
                )
            );
        }, $values, $unique_by, $update);
    }

    /**
     * @inheritDoc
     * @return Promise<int>
     */
    public function increment($column, $amount = 1, array $extra = [])
    {
        return parent::increment($column, $amount, $extra);
    }

    /**
     * @inheritDoc
     * @return Promise<int>
     */
    public function decrement($column, $amount = 1, array $extra = [])
    {
        return parent::decrement($column, $amount, $extra);
    }

    /**
     * @inheritDoc
     * @return Promise<int>
     */
    public function delete($id = null)
    {
        return parent::delete($id);
    }

    /**
     * @inheritDoc
     * @return Promise<null>
     */
    public function truncate()
    {
        return co(function () {
            $this->applyBeforeQueryCallbacks();

            foreach ($this->grammar->compileTruncate($this) as $sql => $bindings) {
                yield $this->connection->statement($sql, $bindings);
            }
        });
    }

    /**
     * @inheritDoc
     * @return Promise<string>
     */
    public function implode($column, $glue = '')
    {
        return $this->pluck($column)
            ->then(function (Collection $collection) use ($glue) {
                return $collection->implode($glue);
            });
    }

    /**
     * @inheritDoc
     * @return Promise<bool>
     */
    public function exists()
    {
        $sql = $this->grammar->compileExists($this);
        $bindings = $this->getBindings();
        $this->option->setUseWrite($this->useWritePdo);
        return $this->connection
            ->select($sql, $bindings, $this->option)
            ->then(function (array $results) {
                // If the results has rows, we will get the row and see if the exists column is a
                // boolean true. If there is no results for this query we will return false as
                // there are no rows for this query at all and we can return that info here.
                if (isset($results[0])) {
                    $results = (array) $results[0];
                    return (bool) $results['exists'];
                }
                return false;
            });
    }

    /**
     * @inheritDoc
     * @return Promise<bool>
     */
    public function doesntExist()
    {
        return $this->exists()
            ->then(function ($result) {
                return false === $result;
            });
    }

    /**
     * @inheritDoc
     * @return Promise<mixed>
     */
    public function existsOr(Closure $callback)
    {
        return $this->exists()
            ->then(function ($result) use ($callback) {
                return $result ?: $callback();
            });
    }

    /**
     * @inheritDoc
     * @return Promise<mixed>
     */
    public function doesntExistOr(Closure $callback)
    {
        return $this->exists()
            ->then(function ($result) use ($callback) {
                return $result ? $callback() : !$result;
            });
    }

    /**
     * @inheritDoc
     * @return Promise<int>
     */
    public function count($columns = '*')
    {
        return $this->aggregate(__FUNCTION__, Arr::wrap($columns))
            ->then(function ($result) {
                return (int) $result;
            });
    }

    /**
     * @inheritDoc
     * @return Promise<mixed>
     */
    public function min($column)
    {
        return parent::min($column);
    }

    /**
     * @inheritDoc
     * @return Promise<mixed>
     */
    public function max($column)
    {
        return parent::max($column);
    }

    /**
     * @inheritDoc
     * @return Promise<mixed>
     */
    public function sum($column)
    {
        return $this->aggregate(__FUNCTION__, [$column])
            ->then(function ($result) {
                return $result ?: 0;
            });
    }

    /**
     * @inheritDoc
     * @return Promise<mixed>
     */
    public function avg($column)
    {
        return parent::avg($column);
    }

    /**
     * @inheritDoc
     * @return Promise<mixed>
     */
    public function average($column)
    {
        return $this->avg($column);
    }

    /**
     * @inheritDoc
     * @return Promise<\Illuminate\Support\Collection>
     */
    public function pluck($column, $key = null)
    {
        return $this->runSelect()
            ->then(function (array $results) use ($column, $key) {
                // First, we will need to select the results of the query accounting for the
                // given columns / key. Once we have the results, we will be able to take
                // the results and get the exact data that was requested for the query.
                $query_result = $this->onceWithColumns(
                    is_null($key) ? [$column] : [$column, $key],
                    function () use ($results) {
                        return $this->processor->processSelect($this, $results);
                    }
                );

                if (empty($query_result)) {
                    return collect();
                }

                // If the columns are qualified with a table or have an alias, we cannot use
                // those directly in the "pluck" operations since the results from the DB
                // are only keyed by the column itself. We'll strip the table out here.
                $column = $this->stripTableForPluck($column);

                $key = $this->stripTableForPluck($key);

                return is_array($query_result[0])
                    ? $this->pluckFromArrayColumn($query_result, $column, $key)
                    : $this->pluckFromObjectColumn($query_result, $column, $key);
            });
    }

    /**
     * @inheritDoc
     * @return Promise<null|\stdClass>
     */
    public function first($columns = ['*'])
    {
        return $this->take(1)
            ->runSelectOne();
    }

    /**
     * @inheritDoc
     * @return Promise<null|\stdClass>
     */
    public function sole($columns = ['*'])
    {
        return $this->take(2)
            ->get($columns)
            ->then(function (Collection $collection) {
                if ($collection->isEmpty()) {
                    throw new RecordsNotFoundException;
                }

                if ($collection->count() > 1) {
                    throw new MultipleRecordsFoundException;
                }

                return $collection->first();
            });
    }

    /**
     * @inheritDoc
     * $callback将以协程方式运行
     * @return Promise<bool>
     */
    public function chunk($count, callable $callback)
    {
        $count = (int) $count;
        $this->enforceOrderBy();

        return co(function ($count, callable $callback) {
            $page = 1;
            do {
                // We'll execute the query for the given page and get the results. If there are
                // no results we can just break and return from here. When there are results
                // we will call the callback with the current chunk of these results here.
                /** @var Collection $results */
                $results = yield $this->forPage($page, $count)->get();

                if (0 === $count_result = $results->count()) {
                    break;
                }

                // On each chunk result set, we will pass them to the callback and then let the
                // developer take care of everything within the callback, which allows us to
                // keep the memory low for spinning through large result sets for working.
                if (false === yield $callback($results, $page)) {
                    // return false;
                    yield returnValue(false);
                }

                unset($results);
                $page++;
            } while ($count_result === $count);

            // return true;
            yield returnValue(true);
        }, $count, $callback);
    }

    /**
     * @inheritDoc
     * @return Promise<\Illuminate\Support\Collection>
     */
    public function chunkMap(callable $callback, $count = 1000)
    {
        $collection = Collection::make();

        return $this->chunk($count, function (Collection $items) use ($collection, $callback) {
            $items->each(function ($item) use ($collection, $callback) {
                $collection->push($callback($item));
            });
        })->then(function () use ($collection) {
            return $collection;
        });
    }

    /**
     * @inheritDoc
     * $callback将以协程方式运行
     * @return Promise<bool>
     */
    public function chunkById($count, callable $callback, $column = null, $alias = null)
    {
        $count = (int) $count;
        $column = null === $column ? $this->defaultKeyName() : $column;
        $column = (string) $column; 

        $alias = null === $alias ? $column : $alias;
        $alias = (string) $alias;

        return co(function ($count, callable $callback, $column, $alias) {
            $last_id = null;
            $page = 1;

            do {
                $clone = clone $this;
                // We'll execute the query for the given page and get the results. If there are
                // no results we can just break and return from here. When there are results
                // we will call the callback with the current chunk of these results here.
                $results = yield $clone->forPageAfterId($count, $last_id, $column)->get();

                if (0 === $count_result = $results->count()) {
                    break;
                }

                // On each chunk result set, we will pass them to the callback and then let the
                // developer take care of everything within the callback, which allows us to
                // keep the memory low for spinning through large result sets for working.
                if (false === yield $callback($results, $page)) {
                    // return false;
                    yield returnValue(false);
                }

                if (null === $last_id = $results->last()->{$alias}) {
                    throw new BadMethodCallException("The chunkById operation was aborted because the [{$alias}] column is not present in the query result.");
                }

                unset($results);
                $page++;
            } while ($count_result === $count);

            // return true;
            yield returnValue(true);
        }, $count, $callback, $column, $alias);
    }

    /**
     * @inheritDoc
     * @throws BadMethodCallException
     */
    public function lazy($chunk_size = 1000)
    {
        throw new BadMethodCallException();
    }

    /**
     * @inheritDoc
     * @throws BadMethodCallException
     */
    public function lazyById($chunkSize = 1000, $column = null, $alias = null)
    {
        throw new BadMethodCallException();
    }

    /**
     * @inheritDoc
     * @throws BadMethodCallException
     */
    public function lazyByIdDesc($chunkSize = 1000, $column = null, $alias = null)
    {
        throw new BadMethodCallException();
    }

    /**
     * @inheritDoc
     * $callback将以协程方式运行
     * @return Promise<bool>
     */
    public function each(callable $callback, $count = 1000)
    {
        return $this->chunk($count, function ($results) use ($callback) {
            foreach ($results as $key => $value) {
                if (false === yield $callback($value, $key)) {
                    // return false;
                    yield returnValue(false);
                }
            }
        });
    }

    /**
     * @inheritDoc
     * $callback将以协程方式运行
     * @return Promise<bool>
     */
    public function eachById(callable $callback, $count = 1000, $column = null, $alias = null)
    {
        return $this->chunkById($count, function ($results, $page) use ($callback, $count) {
            foreach ($results as $key => $value) {
                if (false === yield $callback($value, (($page - 1) * $count) + $key)) {
                    // return false;
                    yield returnValue(false);
                }
            }
        }, $column, $alias);
    }

    /**
     * @inheritDoc
     * @return Promise<mixed>
     */
    public function aggregate($function, $columns = ['*'])
    {
        return $this->cloneWithout($this->unions || $this->havings ? [] : ['columns'])
            ->cloneWithoutBindings($this->unions || $this->havings ? [] : ['select'])
            ->setAggregate($function, $columns)
            ->get($columns)
            ->then(function (Collection $results) {
                if (!$results->isEmpty()) {
                    return array_change_key_case((array) $results[0])['aggregate'];
                }
            });
    }

    /**
     * @inheritDoc
     * @return Promise<float|int>
     */
    public function numericAggregate($function, $columns = ['*'])
    {
        return $this->aggregate($function, $columns)
            ->then(function ($result) {
                // If there is no result, we can obviously just return 0 here. Next, we will check
                // if the result is an integer or float. If it is already one of these two data
                // types we can just return the result as-is, otherwise we will convert this.
                if (!$result) {
                    return 0;
                }

                if (is_int($result) || is_float($result)) {
                    return $result;
                }

                // If the result doesn't contain a decimal place, we will assume it is an int then
                // cast it to one. When it does we will cast it to a float since it needs to be
                // cast to the expected data type for the developers out of pure convenience.
                return strpos((string) $result, '.') === false
                    ? (int) $result : (float) $result;
            });
    }

    /**
     * Run the query as a "select" statement against the connection.
     *
     * @return Promise<\stdClass[]>
     */
    protected function runSelect()
    {
        return $this->connection->select(
            $this->toSql(),
            $this->getBindings(),
            $this->option->setUseWrite($this->useWritePdo)
        );
    }

    /**
     * 只取一项
     *
     * @return Promise<\stdClass>
     */
    protected function runSelectOne()
    {
        return $this->connection->selectOne(
            $this->toSql(),
            $this->getBindings(),
            $this->option->setUseWrite($this->useWritePdo)
        );
    }
}
