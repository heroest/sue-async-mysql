<?php

namespace Sue\Async\Mysql\Tests;

use Sue\Async\Mysql\Tests\BaseTest;
use Sue\Async\Mysql\Tests\Data\User;
use Illuminate\Database\MultipleRecordsFoundException;
use Illuminate\Database\RecordsNotFoundException;
use Illuminate\Support\Collection;

class QueryTest extends BaseTest
{
    public function testWhereInt()
    {
        $promise = $this->connection()->table('users')->where('id', 1)->first();
        $value = $this->promiseValue($promise);
        $this->assertNotNull($value);
        $this->assertEquals($value->name, 'foo');
    }

    public function testWhereString()
    {
        $promise = $this->connection()->table('users')->where('name', 'foo')->first();
        $value = $this->promiseValue($promise);
        $this->assertNotNull($value);
        $this->assertEquals($value->age, 10);
    }

    public function testWithStringQuote()
    {
        $promise = $this->connection()
            ->table('users')
            ->where('name', "wang'wu")
            ->first();
        $value = $this->promiseValue($promise);
        $this->assertEquals(5, $value->age);
    }

    public function testWithQuestionMark()
    {
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'xihai?')
            ->first();
        $value = $this->promiseValue($promise);
        $this->assertEquals(13, $value->age);
    }

    public function testWithJoin()
    {
        $promise = $this->connection()
            ->table('users', 'u')
            ->leftJoin('cats as c', 'c.user_id', '=', 'u.id')
            ->where('c.name', 'lefay')
            ->select('u.name')
            ->first();
        $value = $this->promiseValue($promise);
        $this->assertEquals('lisi', $value->name);
    }

    public function testWithSubQuery()
    {
        $promise = $this->connection()
            ->table('users', 'u')
            ->whereExists(function ($query) {
                $query->from('cats', 'c')
                    ->whereRaw('u.id = c.user_id')
                    ->where('c.name', 'lefay');
            })
            ->select('u.name')
            ->get();
        $value = $this->promiseValue($promise);
        $this->assertTrue($value instanceof Collection);
        $this->assertEquals(1, $value->count());
        $row = $value->first();
        $this->assertEquals('lisi', $row->name);
    }

    public function testPluck()
    {
        $data = User::data();
        $promise = $this->connection()->table('users')->pluck('name');
        /** @var Collection $value */
        $value = $this->promiseValue($promise);
        $this->assertTrue($value instanceof Collection);
        $this->assertEquals(count($data), $value->count());
        $list = $value->toArray();
        $names = array_column($data, 'name');
        foreach ($names as $name) {
            $this->assertContains($name, $list);
        }
    }

    public function testExists()
    {
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'foo')
            ->exists();
        $value = $this->promiseValue($promise);
        $this->assertTrue($value);
    }

    public function testNotExists()
    {
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'not foo')
            ->doesntExist();
        $value = $this->promiseValue($promise);
        $this->assertTrue($value);
    }

    public function testExistOrWithTrue()
    {
        $called = false;
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'foo')
            ->existsOr(function () use (&$called) {
                $called = true;
            });
        $value = $this->promiseValue($promise);
        $this->assertTrue($value);
        $this->assertFalse($called);
    }

    public function testExistOrWithFalse()
    {
        $called = false;
        $num = 907;
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'not foo')
            ->existsOr(function () use (&$called, $num) {
                $called = true;
                return $num;
            });
        $value = $this->promiseValue($promise);
        $this->assertEquals($num, $value);
        $this->assertTrue($called);
    }

    public function testDoesntExistOrWithTrue()
    {
        $called = false;
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'not foo')
            ->doesntExistOr(function () use (&$called) {
                $called = true;
            });
        $value = $this->promiseValue($promise);
        $this->assertTrue($value);
        $this->assertFalse($called);
    }

    public function testDoesntExistOrWithFalse()
    {
        $called = false;
        $num = 907;
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'foo')
            ->doesntExistOr(function () use (&$called, $num) {
                $called = true;
                return $num;
            });
        $value = $this->promiseValue($promise);
        $this->assertEquals($num, $value);
        $this->assertTrue($called);
    }

    public function testSole()
    {
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'foo')
            ->sole();
        $value = $this->promiseValue($promise);
        $this->assertEquals('foo', $value->name);
    }

    public function testSoleWithRecordDoesntExist()
    {
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'not foo')
            ->sole();
        $value = $this->promiseValue($promise);
        $this->assertTrue($value instanceof RecordsNotFoundException);
    }

    public function testSoleWithMultipleRecords()
    {
        $promise = $this->connection()
            ->table('users')
            ->where('age', 20)
            ->sole();
        $value = $this->promiseValue($promise);
        $this->assertTrue($value instanceof MultipleRecordsFoundException);
    }

    public function testFirst()
    {
        $promise = $this->connection()
            ->table('users')
            ->orderByDesc('id')
            ->first();
        $user = $this->promiseValue($promise);
        $data = User::data();
        $last = end($data);
        $this->assertEquals($last['name'], $user->name);
    }

    public function testCount()
    {
        $promise = $this->connection()
            ->table('users')
            ->count();
        $value = $this->promiseValue($promise);
        $this->assertEquals(count(User::data()), $value);
    }

    public function testMin()
    {
        $promise = $this->connection()
            ->table('users')
            ->min('age');
        $value = $this->promiseValue($promise);
        $min_age = min(array_column(User::data(), 'age'));
        $this->assertEquals($min_age, $value);
    }

    public function testMax()
    {
        $promise = $this->connection()
            ->table('users')
            ->max('age');
        $value = $this->promiseValue($promise);
        $max_age = max(array_column(User::data(), 'age'));
        $this->assertEquals($max_age, $value);
    }

    public function testSum()
    {
        $promise = $this->connection()
            ->table('users')
            ->sum('age');
        $value = $this->promiseValue($promise);
        $sum_age = array_sum(array_column(User::data(), 'age'));
        $this->assertEquals($sum_age, $value);
    }

    public function testAvg()
    {
        $promise = $this->connection()
            ->table('users')
            ->avg('age');
        $value = $this->promiseValue($promise);
        $avg_age = array_sum(array_column(User::data(), 'age')) / count(User::data());
        $value = bcadd($value, '0', 2);
        $avg_age = bcadd($avg_age, '0', 2);
        $this->assertEquals($avg_age, $value);
    }
}