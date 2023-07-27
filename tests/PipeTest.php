<?php

namespace Sue\Async\Mysql\Tests;

use Throwable;
use RuntimeException;
use ReflectionObject;
use Psr\Log\Test\TestLogger;
use React\Promise\Timer\TimeoutException;
use Illuminate\Database\QueryException;
use Sue\Async\Mysql\Exceptions\PipeException;
use Sue\Async\Mysql\Pipe;
use Sue\Async\Mysql\Connection;
use Sue\Async\Mysql\PipeManager;
use Sue\Async\Mysql\Query\QueryOption;
use Sue\Async\Mysql\Query\QueryManager;
use Sue\Async\Mysql\Tests\BaseTest;
use Illuminate\Support\Arr;

use function Sue\EventLoop\setTimeout;
use function Sue\Coroutine\SystemCall\pause;

class PipeTest extends BaseTest
{
    public function testCancelBeforeQuery()
    {
        $promise = $this->connection()->unprepared('select 1');
        $promise->done(null, function ($e) {
            $this->assertTrue($e instanceof QueryException);
        });
        $promise->cancel();
        $value = $this->promiseValue($promise);
        $this->assertTrue($value instanceof QueryException);
    }

    public function testTimeout()
    {
        $called = false;
        $st = time();
        $timeout = 2;
        $option = (new QueryOption())->setTimeout($timeout);
        $promise = $this->connection()->unprepared("SELECT SLEEP(5);", $option);
        $promise->done(null, function (Throwable $e) use (&$called, $st, $timeout) {
            $called = true;
            $this->assertTrue($e instanceof QueryException);
            $this->assertTrue($e->getPrevious() instanceof TimeoutException);
            $time_used = time() - $st;
            $this->assertLessThanOrEqual($timeout + 1, $time_used);
        });
        $this->promiseValue($promise, 10);
        $this->assertTrue($called);
        $time_used = time() - $st;
        $this->assertLessThanOrEqual($timeout + 1, $time_used);
    }

    public function testBuilderTimeout()
    {
        $called = false;
        $st = time();
        $timeout = 2;
        $promise = $this->connection()
            ->table('users')
            ->setTimeout($timeout)
            ->selectRaw('SLEEP(5)')
            ->first();
        $promise->done(null, function (Throwable $e) use (&$called, $st, $timeout) {
            $called = true;
            $this->assertTrue($e instanceof QueryException);
            $this->assertTrue($e->getPrevious() instanceof TimeoutException);
            $time_used = time() - $st;
            $this->assertLessThanOrEqual($timeout + 1, $time_used);
        });
        $this->promiseValue($promise, 10);
        $time_used = time() - $st;
        $this->assertLessThanOrEqual($timeout + 1, $time_used);
        $this->assertTrue($called);
    }

    public function testNotTimeout()
    {
        $bit = 0;
        $st = time();
        $timeout = 5;
        $option = (new QueryOption())->setTimeout($timeout);
        $promise = $this->connection()->unprepared("SELECT SLEEP(1);", $option);
        $promise->done(
            function ($v) use (&$bit) {
                $bit |= 1;
            },
            function ($e) use (&$bit) {
                $bit |= 2;
            }
        );
        $this->promiseValue($promise, 10);
        $this->assertEquals(1, $bit);
        $time_used = time() - $st;
        $this->assertLessThanOrEqual($timeout + 0.5, $time_used);
    }

    public function testCancelBeforeTimeout()
    {
        $bit = 0;
        $st = time();
        $timeout = 5;
        $option = (new QueryOption())->setTimeout($timeout);
        $promise = $this->connection()->unprepared("SELECT SLEEP(3);", $option);
        $promise->done(
            function ($v) use (&$bit) {
                $bit |= 1;
            },
            function ($e) use (&$bit) {
                $bit |= 2;
                $this->assertTrue($e instanceof QueryException);
            }
        );
        setTimeout(1, [$promise, 'cancel']);
        $exception = $this->promiseValue($promise, 10);
        $this->assertEquals(2, $bit);
        $time_used = time() - $st;
        $this->assertLessThanOrEqual(1.5, $time_used);
        $this->assertTrue($exception instanceof QueryException);
    }

    public function testWaitingList()
    {
        $results = $this->coroutineValue(function () {
            $count = 15;
            $promises = [];
            $connection = $this->connection();
            while ($count--) {
                $promises[] = $connection->table('users')->where('id', 1)->first();
            }
            return yield $promises;
        });
        $this->assertTrue(is_array($results));
        $names = [];
        foreach ($results as $user) {
            $names[$user->name] = true;
        }
        $this->assertCount(1, $names);
    }

    public function testWaitingListOverCap()
    {
        $results = $this->coroutineValue(function () {
            $count = 16;
            $promises = [];
            $connection = $this->connection();
            while ($count--) {
                $promises[] = $connection->table('users')->where('id', 1)->first();
            }
            return yield $promises;
        }, 2);
        $this->assertTrue(is_array($results));
        $this->assertCount(16, $results);
        $names = [];
        $exception = null;
        $count_success = 0;
        foreach ($results as $mixed) {
            if ($mixed instanceof Throwable) {
                $exception = $mixed;
            } else {
                $count_success++;
                $names[$mixed->name] = true;
            }
        }
        $this->assertCount(1, $names);
        $this->assertEquals(15, $count_success);
        $this->assertTrue($exception instanceof QueryException);
        $this->assertEquals(new PipeException('waiting list is full'), $exception->getPrevious());
    }

    public function testPipeRecycleDebounce()
    {
        $connection = $this->connection();
        $connection->setPipeRecycleDebounceSeconds(3);
        $this->coroutineValue(function () use ($connection) {
            $count = 3;
            $promises = [];
            do {
                $promises[] = $connection->table('users')->first();
            } while (--$count);
            yield $promises;
            $this->assertEquals(3, $this->countPipeIdle('read'));
            yield pause(5);
            $this->assertEquals(1, $this->countPipeIdle('read'));
        }, 10);
    }

    public function testPipeWithoutRecycleDebounce()
    {
        $connection = $this->connection();
        $connection->setPipeRecycleDebounceSeconds(3);

        $pm = $this->getPipeManager($connection);
        $reflection = new ReflectionObject($pm);
        $prop = $reflection->getProperty('pendings');
        $prop->setAccessible(true);
        $pendings  = $prop->getValue($pm);

        $this->coroutineValue(function () use ($connection, $pendings) {
            yield $connection->table('users')->first();
            $this->assertEquals(1, $pendings->count());
            $this->assertEquals(1, $this->countPipeIdle('read'));
            yield pause(5);
            $this->assertEquals(1, $this->countPipeIdle('read'));
            $this->assertEquals(0, $pendings->count());
        }, 10);
    }

    public function testPipeWithPingDebounce()
    {
        $connection = $this->connection();
        $connection->setPipePingDebounceSeconds(3);

        $pm = $this->getPipeManager($connection);
        $pm->setLogger($logger = new TestLogger());
        $reflection = new ReflectionObject($pm);
        $prop = $reflection->getProperty('pendings');
        $prop->setAccessible(true);
        /** @var \SplObjectStorage $pendings */
        $pendings  = $prop->getValue($pm);
        $this->coroutineValue(function () use ($connection, $logger, $pendings) {
            $connection->table('users')->first();
            $this->assertEquals(1, $pendings->count());
            yield pause(5);
            $this->assertTrue($logger->hasRecordThatMatches('#^pipe:(\d+)\sping#', 'debug'));
            $this->assertEquals(0, $pendings->count());
        }, 10);
    }

    public function testPipeLogThrowable()
    {
        $connection = $this->connection();
        $pm = $this->getPipeManager($connection);
        $reflection = new ReflectionObject($pm);
        $prop = $reflection->getProperty(Pipe::ROLE_READ);
        $prop->setAccessible(true);
        $reads = $prop->getValue($pm);
        $pipe = $reads[0];
        $pipe->setLogger($logger = new TestLogger());

        $reflection = new ReflectionObject($pipe);
        $method = $reflection->getMethod('logThrowable');
        $method->setAccessible(true);
        $exception = new RuntimeException('foo');
        $method->invoke($pipe, $exception);
        $this->assertTrue($logger->hasErrorThatContains('foo'));
        $this->assertTrue($logger->hasErrorThatPasses(function ($record) {
            $stacks = Arr::get($record, 'context._trace_stacks', null);
            return is_array($stacks) and Arr::isList($stacks);
        }));
    }

    private function countPipeIdle(string $role)
    {
        $connection = $this->connection();
        $pm = $this->getPipeManager($connection);
        $reflection = new ReflectionObject($pm);

        //获取pipe
        $prop = $reflection->getProperty($role);
        $prop->setAccessible(true);
        $pipes = $prop->getValue($pm);

        //设置idle timeout
        $prop = $reflection->getProperty('idleTimeout');
        $prop->setAccessible(true);
        $prop->setValue($pm, 1);
        
        $count = 0;
        foreach ($pipes as $pipe) {
            /** @var Pipe $pipe */
            if ($pipe->inState(Pipe::STATE_IDLE)) {
                $count++;
            }
        }
        return $count;
    }

    /**
     * 获取QueryManager
     *
     * @param Connection $connection
     * @return QueryManager
     */
    private function getQueryManager(Connection $connection)
    {
        $reflection = new ReflectionObject($connection);
        $prop = $reflection->getProperty('queryManager');
        $prop->setAccessible(true);
        return $prop->getValue($connection);
    }

    /**
     * 获取pipeManager
     *
     * @param Connection $connection
     * @return PipeManager
     */
    private function getPipeManager(Connection $connection)
    {
        $qm = $this->getQueryManager($connection);
        return $qm->getPipeManager();
    }
}
