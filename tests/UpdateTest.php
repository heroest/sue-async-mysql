<?php

namespace Sue\Async\Mysql\Tests;

use Throwable;
use Illuminate\Support\Str;
use Illuminate\Database\QueryException;
use Sue\Async\Mysql\Tests\BaseTest;

class UpdateTest extends BaseTest
{
    public function testUpdateOne()
    {
        $name = Str::random(10);
        $promise = $this->connection()
            ->table('users')
            ->where('id', 1)
            ->update(['name' => $name]);
        $value = $this->promiseValue($promise);
        $this->assertEquals(1, $value);

        $promise = $this->connection()->table('users')->where('id', 1)->first();
        $value = $this->promiseValue($promise);
        $this->assertEquals($name, $value->name);
    }

    public function testUpdateMultiple()
    {
        $name = Str::random(10);
        $promise = $this->connection()
            ->table('users')
            ->where('age', 20)
            ->update(['name' => $name]);
        $updated = $this->promiseValue($promise);
        $this->assertEquals(2, $updated);

        $promise = $this->connection()->table('users')->where('age', 20)->first();
        $value = $this->promiseValue($promise);
        $this->assertEquals($name, $value->name);
    }

    public function testIncrement()
    {
        $step = random_int(1, 5);
        $user_id = random_int(1000, 9999);
        $actual = $this->coroutineValue(function () use ($step, $user_id) {
            $before = yield $this->connection()
                ->table('cats')
                ->where('name', 'chino')
                ->first();

            yield $this->connection()
                ->table('cats')
                ->where('name', 'chino')
                ->increment('age', $step, ['user_id' => $user_id]);

            $after = yield $this->connection()
                ->table('cats')
                ->where('name', 'chino')
                ->first();
            $this->assertEquals($before->age + $step, $after->age);
            $this->assertEquals($after->user_id, $user_id);
            return $step;
        });
        $this->assertEquals($step, $actual);
    }

    public function testDecrement()
    {
        $step = random_int(1, 5);
        $user_id = random_int(1000, 9999);
        $actual = $this->coroutineValue(function () use ($step, $user_id) {
            $before = yield $this->connection()
                ->table('cats')
                ->where('name', 'chino')
                ->first();

            yield $this->connection()
                ->table('cats')
                ->where('name', 'chino')
                ->decrement('age', $step, ['user_id' => $user_id]);

            $after = yield $this->connection()
                ->table('cats')
                ->where('name', 'chino')
                ->first();

            $this->assertEquals($before->age - $step, $after->age);
            $this->assertEquals($after->user_id, $user_id);
            return $step;
        });
        $this->assertEquals($step, $actual);
    }

    public function testUpdateOrInsertWithExists()
    {
        $age = random_int(1000, 9999);
        $actual = $this->coroutineValue(function () use ($age) {
            $bool = yield $this->connection()
                ->table('cats')
                ->updateOrInsert(['name' => 'chino'], ['age' => $age]);
            $cat = yield $this->connection()
                ->table('cats')
                ->where('name', 'chino')
                ->first();
            $this->assertTrue($bool);
            $this->assertEquals($age, $cat->age);
            return $age;
        });
        $this->assertEquals($age, $actual);
    }

    public function testUpdateOrInsertWithNotExists()
    {
        $age = random_int(1000, 9999);
        $name = Str::random(10);
        $actual = $this->coroutineValue(function () use ($age, $name) {
            $bool = yield $this->connection()
                ->table('cats')
                ->updateOrInsert(['name' => $name], ['age' => $age]);

            $cat = yield $this->connection()
                ->table('cats')
                ->where('name', $name)
                ->first();
            $this->assertTrue($bool);
            $this->assertEquals($age, $cat->age);
            $this->assertEquals($name, $cat->name);
            $this->assertEquals(0, $cat->user_id);
            return [$name, $age];
        });
        $this->assertTrue(is_array($actual));
        $this->assertEquals([$name, $age], $actual);
    }

    public function testUpsertWithInsert()
    {
        $name = Str::random(32);
        $age = random_int(1000, 9999);
        $actual = $this->coroutineValue(function () use ($name, $age) {
            $updated = yield $this->connection()
                ->table('users')
                ->upsert(['name' => $name, 'age' => $age]);
            $this->assertEquals(1, $updated);

            $exists = yield $this->connection()
                ->table('users')
                ->where(['name' => $name, 'age' => $age])
                ->exists();
            $this->assertTrue($exists);
            return [$name, $age];
        });
        $this->assertEquals([$name, $age], $actual);
    }

    public function testUpsertWithInsertOnly()
    {
        $throwable = null;
        $this->coroutineValue(function () use (&$throwable) {
            try {
                yield $this->connection()
                    ->table('users')
                    ->upsert(['id' => 1, 'name' => 'chino', 'age' => 20], '', []);
                return true;
            } catch (Throwable $e) {
                $throwable = $e;
            }
        });
        $this->assertTrue($throwable instanceof QueryException);
    }

    public function testUpsertWithUpdate()
    {
        $throwable = null;
        $name = Str::random(16);
        $age = random_int(1000, 9999);
        $actual = $this->coroutineValue(function () use (&$throwable, $name, $age) {
            try {
                yield $this->connection()
                    ->table('users')
                    ->upsert(['id' => 1, 'name' => $name, 'age' => $age]);
                return [$name, $age];
            } catch (Throwable $e) {
                $throwable = $e;
            }
        });
        $this->assertNull($throwable);
        $this->assertEquals([$name, $age], $actual);
    }
}
