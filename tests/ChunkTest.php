<?php

namespace Sue\Async\Mysql\Tests;

use stdClass;
use RuntimeException;
use Sue\Async\Mysql\Tests\BaseTest;
use Sue\Async\Mysql\Tests\Custom\FetchIdSystemCall;
use Sue\Async\Mysql\Tests\Data\User;
use Sue\Coroutine\SystemCall;
use Illuminate\Support\Collection;
use Illuminate\Support\LazyCollection;

class ChunkTest extends BaseTest
{
    public function testCursor()
    {
        $promise = $this->connection()
            ->table('users')
            ->cursor();
        $collection = $this->promiseValue($promise);
        $this->assertTrue($collection instanceof LazyCollection);
        $this->assertEquals(count(User::data()), $collection->count());
    }

    public function testChunk()
    {
        $names = array_column(User::data(), 'name');
        $names = array_flip($names);
        $promise = $this->connection()
            ->table('users')
            ->orderBy('id')
            ->chunk(2, function (Collection $chunk) use (&$names) {
                $this->assertLessThanOrEqual(2, $chunk->count());
                foreach ($chunk as $row) {
                    unset($names[$row->name]);
                }
            });
        $value = $this->promiseValue($promise);
        $this->assertTrue($value);
        $this->assertCount(0, $names);
    }

    public function testChunkWithCoroutine()
    {
        $names = array_column(User::data(), 'name');
        $names = array_flip($names);
        $hash = null;
        $promise = $this->connection()
            ->table('users')
            ->orderBy('id')
            ->chunk(2, function (Collection $chunk) use (&$names, &$hash) {
                $hash = yield new FetchIdSystemCall();
                $this->assertLessThanOrEqual(2, $chunk->count());
                foreach ($chunk as $row) {
                    unset($names[$row->name]);
                }
            });
        $value = $this->promiseValue($promise);
        $this->assertTrue($value);
        $this->assertCount(0, $names);
        $this->assertNotNull($hash);
    }

    public function testChunkWithError()
    {
        $total = 0;
        $promise = $this->connection()
            ->table('users')
            ->orderBy('id')
            ->chunk(2, function (Collection $chunk) use (&$total) {
                $total += $chunk->count();
                throw new RuntimeException('chunkError');
            });
        $exception = $this->promiseValue($promise);
        $this->assertEquals(2, $total);
        $this->assertEquals(new RuntimeException('chunkError'), $exception);
    }

    public function testChunkById()
    {
        $total = 0;
        $callback = function ($users) use (&$total) {
            $total += $users->count();
        };
        $promise = $this->connection()
            ->table('users')
            ->select('id as uid', 'name')
            ->chunkById(2, $callback, 'id', 'uid');
        $value = $this->promiseValue($promise, 5);
        $this->assertTrue($value);
        $data = User::data();
        $this->assertEquals(count($data), $total);
    }

    public function testChunkByIdWithCoroutine()
    {
        $total = 0;
        $st = microtime(true);
        $sleep = 2;
        $chunk_size = 2;
        $callback = function ($users) use (&$total, $sleep) {
            $total += $users->count();
            yield SystemCall\sleep($sleep);
        };
        $promise = $this->connection()
            ->table('users')
            ->select('id as uid', 'name')
            ->chunkById($chunk_size, $callback, 'id', 'uid');
        $value = $this->promiseValue($promise, 10);
        $this->assertTrue($value);
        $data = User::data();
        $this->assertEquals(count($data), $total);
        $expected = ceil(count($data) / $chunk_size) * $sleep;
        $time_used = (float) bcsub(microtime(true), $st, 4);
        $this->assertGreaterThanOrEqual($expected, $time_used);
    }

    public function testChunkByIdWithError()
    {
        $chunk_size = 2;
        $total = 0;
        $callback = function ($users) use (&$total) {
            $total += $users->count();
            throw new RuntimeException('chunkIdError');
        };
        $promise = $this->connection()
            ->table('users')
            ->select('id as uid', 'name')
            ->chunkById($chunk_size, $callback, 'id', 'uid');
        $exception = $this->promiseValue($promise, 10);
        $this->assertEquals(new RuntimeException('chunkIdError'), $exception);
        $this->assertEquals($total, $chunk_size);
    }

    public function testEach()
    {
        $total = 0;
        $this->connection()->flushQueryLog();
        $this->connection()->enableQueryLog();
        $chunk_size = 2;
        $callback = function ($user) use (&$total) {
            $this->assertTrue($user instanceof stdClass);
            $total++;
        };
        $promise = $this->connection()
            ->table('users')
            ->orderBy('id')
            ->each($callback, $chunk_size);
        $bool = $this->promiseValue($promise, 5);
        $this->assertTrue($bool);
        $data = User::data();
        $count_data = count($data);
        $this->assertEquals($count_data, $total);
        $logs = $this->connection()->getQueryLog();
        $this->connection()->flushQueryLog();
        $expected = (0 === $count_data % $chunk_size)
            ? ($count_data / $chunk_size + 1) //extra query assure not more data
            : ($count_data / $chunk_size);
        $this->assertEquals($expected, count($logs));
    }

    public function testEachWithCoroutine()
    {
        $total = 0;
        $this->connection()->flushQueryLog();
        $this->connection()->enableQueryLog();
        $chunk_size = 2;
        $hash = null;
        $callback = function ($user) use (&$total, &$hash) {
            $hash = yield new FetchIdSystemCall();
            $this->assertTrue($user instanceof stdClass);
            $total++;
        };
        $promise = $this->connection()
            ->table('users')
            ->orderBy('id')
            ->each($callback, $chunk_size);
        $bool = $this->promiseValue($promise, 5);
        $this->assertTrue($bool);
        $data = User::data();
        $count_data = count($data);
        $this->assertEquals($count_data, $total);
        $logs = $this->connection()->getQueryLog();
        $this->connection()->flushQueryLog();
        $expected = (0 === $count_data % $chunk_size)
            ? ($count_data / $chunk_size + 1) //extra query assure not more data
            : ($count_data / $chunk_size);
        $this->assertEquals($expected, count($logs));
    }

    public function testEachWithError()
    {
        $total = 0;
        $callback = function ($user) use (&$total) {
            $total++;
            $this->assertTrue($user instanceof stdClass);
            throw new RuntimeException('eachError');
        };
        $promise = $this->connection()
            ->table('users')
            ->orderBy('id')
            ->each($callback, 100);
        $exception = $this->promiseValue($promise, 15);
        $this->assertEquals(new RuntimeException('eachError'), $exception);
        $this->assertEquals(1, $total);
    }

    public function testEachByIdWithCoroutine()
    {
        $names = array_column(User::data(), 'name');
        $names = array_flip($names);
        $hash = null;
        $promise = $this->connection()
            ->table('users')
            ->select('id as uid', 'name')
            ->orderBy('id')
            ->eachById(function ($user) use (&$names, &$hash) {
                $hash = yield new FetchIdSystemCall();
                unset($names[$user->name]);
            }, 2, 'id', 'uid');
        $value = $this->promiseValue($promise);
        $this->assertTrue($value);
        $this->assertCount(0, $names);
        $this->assertNotNull($hash);
    }
}
