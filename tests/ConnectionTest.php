<?php

namespace Sue\Async\Mysql\Tests;

use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\Log;
use Sue\Async\Mysql\Tests\Data\User;

use function Sue\EventLoop\setTimeout;

class ConnectionTest extends BaseTest
{
    public function testConnectionDisconnectDuringQuery()
    {
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'foo')
            ->selectRaw('SLEEP(10)')
            ->first();
        setTimeout(1, function () {
            $this->connection()->disconnect();
        });
        $value = $this->promiseValue($promise, 5);
        $this->assertTrue($value instanceof QueryException);
    }

    public function testConnectionDisconnectBeforeQuery()
    {
        $promise = $this->connection()
            ->table('users')
            ->where('name', 'foo')
            ->selectRaw('SLEEP(10)')
            ->first();
        $this->connection()->disconnect();
        $value = $this->promiseValue($promise, 1);
        $this->assertTrue($value instanceof QueryException);
    }

    public function testConnectionReconnect()
    {
        $this->coroutineValue(function () {
            $data = User::data();
            //first
            shuffle($data);
            $first = $data[0];
            $user = yield $this->connection()
                ->table('users')
                ->where('name', $first['name'])
                ->first();
            $this->assertEquals($user->age, $first['age']);
            Log::debug("got first user");

            $this->connection()->disconnect();

            //2nd reconnect
            shuffle($data);
            $first = $data[0];
            $user = yield $this->connection()
                ->table('users')
                ->where('name', $first['name'])
                ->first();
            Log::debug('got second user');
            $this->assertEquals($user->age, $first['age']);
        }, 15);
    }
}