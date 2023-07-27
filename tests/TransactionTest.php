<?php

namespace Sue\Async\Mysql\Tests;

use Throwable;
use Illuminate\Support\Facades\Event;
use Illuminate\Database\Events as DatabaseEvents;
use Sue\Coroutine\SystemCall;
use Sue\Async\Mysql\Tests\BaseTest;

class TransactionTest extends BaseTest
{
    protected function setUp(): void
    {
        parent::setUp();
        Event::fake();
    }

    public function testCommit()
    {
        $age = $age = random_int(70, 99);
        $age_actual = $this->coroutineValue(function () use ($age) {
            $transaction = yield $this->connection()->beginTransaction();
            Event::assertDispatched(DatabaseEvents\TransactionBeginning::class);
            $updated = yield $this->connection()
                ->table('users')
                ->setTransaction($transaction)
                ->where(['name' => 'foo'])
                ->update(['age' => $age]);
            $this->assertEquals(1, $updated);
            $bool = yield $this->connection()->commit($transaction);
            Event::assertDispatched(DatabaseEvents\TransactionCommitted::class);
            $this->assertTrue(true, $bool);

            $user = yield $this->connection()->table('users')
                ->where(['name' => 'foo'])
                ->first();
            $this->assertEquals($age, $user->age);
            return $user->age;
        });
        Event::assertDispatched(DatabaseEvents\QueryExecuted::class);
        $this->assertEquals($age, $age_actual);
    }

    public function testRollback()
    {
        $age = $age = random_int(70, 99);
        $age_actual = $this->coroutineValue(function () use ($age) {
            $transaction = yield $this->connection()->beginTransaction();
            Event::assertDispatched(DatabaseEvents\TransactionBeginning::class);
            $updated = yield $this->connection()
                ->table('users')
                ->setTransaction($transaction)
                ->where(['name' => 'foo'])
                ->update(['age' => $age]);
            $this->assertEquals(1, $updated);

            //transaction read
            $user = yield $this->connection()->table('users')
                ->setTransaction($transaction)
                ->where(['name' => 'foo'])
                ->first();
            $this->assertEquals($age, $user->age, 'before rollback use transaction');

            //no transaction read
            $user = yield $this->connection()->table('users')
                ->where(['name' => 'foo'])
                ->first();
            $this->assertEquals(10, $user->age, 'before rollback no transaction');

            $bool = yield $this->connection()->rollBack($transaction);
            Event::assertDispatched(DatabaseEvents\TransactionRolledBack::class);
            $this->assertTrue(true, $bool);

            $user = yield $this->connection()->table('users')
                ->where(['name' => 'foo'])
                ->first();
            $this->assertEquals(10, $user->age);
            return $user->age;
        });
        $this->assertEquals(10, $age_actual);
    }

    public function testTransactionWithTimeout()
    {
        $age = $age = random_int(70, 99);
        $throwable = null;
        $this->coroutineValue(function () use ($age, &$throwable) {
            try {
                $transaction = yield $this->connection()->beginTransaction(2);
                Event::assertDispatched(DatabaseEvents\TransactionBeginning::class);
                $updated = yield $this->connection()
                    ->table('users')
                    ->setTransaction($transaction)
                    ->where(['name' => 'foo'])
                    ->update(['age' => $age]);
                $this->assertEquals(1, $updated);
                yield SystemCall\sleep(5);
                yield $this->connection()->commit($transaction);
            } catch (Throwable $e) {
                $throwable = $e;
            }
        }, 10);
        Event::assertNotDispatched(DatabaseEvents\TransactionCommitted::class);
        $this->assertTrue($throwable instanceof Throwable);
    }

    public function testTransactionWithinTime()
    {
        $age = random_int(70, 99);
        $result = $this->coroutineValue(function () use ($age) {
            $transaction = yield $this->connection()->beginTransaction(10);
            Event::assertDispatched(DatabaseEvents\TransactionBeginning::class);
            $updated = yield $this->connection()
                ->table('users')
                ->setTransaction($transaction)
                ->where(['name' => 'foo'])
                ->update(['age' => $age]);
            $this->assertEquals(1, $updated);
            yield SystemCall\sleep(2);
            $bool = yield $this->connection()->commit($transaction);
            Event::assertDispatched(DatabaseEvents\TransactionCommitted::class);
            $this->assertTrue(true, $bool);

            $user = yield $this->connection()->table('users')
                ->where(['name' => 'foo'])
                ->first();
            $this->assertEquals($age, $user->age);
            return $user->age;
        }, 10);
        $this->assertEquals($age, $result);
    }
}
