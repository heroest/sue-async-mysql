<?php

namespace Sue\Async\Mysql\Tests;

use Sue\Async\Mysql\Tests\BaseTest;
use Sue\Async\Mysql\Tests\Data\User;
use Illuminate\Database\QueryException;

class InsertTest extends BaseTest
{
    public function testInsertJson()
    {
        $data = [
            'name' => 'foo',
            'json' => json_encode([
                'class' => __CLASS__,
                'method' => __METHOD__
            ])
        ];
        $promise = $this->connection()
            ->table('dogs')
            ->insertGetId($data);
        $value = $this->promiseValue($promise, 1);
        $this->assertEquals(1, $value);
    }

    public function testInsert()
    {
        $data = [
            'name' => 'foo',
            'json' => json_encode([
                'class' => __CLASS__,
                'method' => __METHOD__
            ])
        ];
        $promise = $this->connection()
            ->table('dogs')
            ->insert($data);
        $bool = $this->promiseValue($promise, 1);
        $this->assertTrue($bool);
    }

    public function testInsertFail()
    {
        $data = [
            'id' => 1,
            'name' => 'foo',
            'age' => 16
        ];
        $promise = $this->connection()
            ->table('users')
            ->insert($data);
        $exception = $this->promiseValue($promise, 1);
        $this->assertTrue($exception instanceof QueryException);
    }

    public function testInsertIgnore()
    {
        $bool = $this->coroutineValue(function () {
            $data = User::data();
            $first = reset($data);
            $copy = $first;
            $copy['id'] = 1;
            $copy['age']++;
            $updated = yield $this->connection()
                ->table('users')
                ->insertOrIgnore($copy);
            $this->assertEquals(0, $updated);
            $user = yield $this->connection()
                ->table('users')
                ->where(['name' => $first['name']])
                ->first();
            $this->assertEquals($first['age'], $user->age);
            return true;
        });
        $this->assertTrue($bool);
    }
}
