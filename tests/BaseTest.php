<?php

namespace Sue\Async\Mysql\Tests;

use Closure;
use Throwable;
use React\Promise\PromiseInterface;
use React\Promise\Promise;
use Orchestra\Testbench\TestCase as OrchestraBaseTest;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Support\Facades\DB;
use Sue\Async\Mysql\AsyncMysqlServiceProvider;
use Sue\Async\Mysql\Connection;
use Sue\Async\Mysql\OptionConst;

use function Sue\Coroutine\co;
use function Sue\EventLoop\cancelTimer;
use function Sue\EventLoop\loop;
use function Sue\EventLoop\setTimeout;

abstract class BaseTest extends OrchestraBaseTest
{
    /** @inheritDoc */
    protected function getPackageProviders($app)
    {
        return [
            AsyncMysqlServiceProvider::class
        ];
    }

    /** @inheritDoc */
    protected function setUp(): void
    {
        parent::setUp();
    }

    /** @inheritDoc */
    protected function defineDatabaseMigrations()
    {
        $basedir = dirname(__DIR__);
        $this->loadMigrationsFrom($basedir . '/database/migrations');
    }

    /**
     * 获取数据库连接
     *
     * @return Connection
     */
    protected function connection(): Connection
    {
        return DB::connection('async_mysql');
    }

    /**
     * 解析promiseValue值
     *
     * @param Promise|PromiseInterface $promise
     * @param integer $interval
     * @return mixed|null;
     */
    protected function promiseValue($promise, float $interval = 1)
    {
        $this->assertTrue($promise instanceof PromiseInterface);
        $value = $timer = null;
        $timer = null;
        $closure = function ($v) use (&$value, &$timer) {
            $value = $v;
            /** @var null|\React\EventLoop\TimerInterface $timer */
            $timer and cancelTimer($timer);
            loop()->stop();
        };
        $promise->done($closure, $closure);
        if (null !== $value) {
            return $value;
        }

        $timeout = false;
        $timer = setTimeout($interval, function () use (&$timeout) {
            $timeout = true;
            loop()->stop();
        });
        loop()->run();
        $this->assertFalse($timeout, "promise timeout: {$interval}");
        return $value;
    }

    protected function coroutineValue(Closure $closure, float $interval = 1)
    {
        $throwable = null;
        $done = false;
        $promise = co($closure);
        $promise->done(function () use (&$done) {
            $done = true;
        }, function (Throwable $e) use (&$throwable, &$done) {
            $done = true;
            $throwable = $e;
        });
        $result = $this->promiseValue($promise, $interval);
        $this->assertTrue($done);
        if ($throwable) {
            throw $throwable;
        }
        return $result;
    }

    protected function defineEnvironment($app)
    {
        $this->setupDatabase($app);
        $this->setupLogger($app);
    }

    private function setupDatabase($app)
    {
        tap($app->make('config'), function (Repository $config) {
            $config->set(
                'database.connections.async_mysql',
                [
                    'driver' => 'async-mysql',
                    'write' => [
                        'host' => [env('DB_HOST', 'localhost')],
                        'username' => env('DB_USERNAME', 'forge'),
                        'password' => env('DB_PASSWORD', ''),
                        OptionConst::NUM_CONNECTIONS => 5,
                    ],
                    'read' => [
                        'host' => explode('|', env('DB_HOST', 'localhost')),
                        'username' => env('DB_USERNAME', 'test'),
                        'password' => env('DB_PASSWORD', 'test'),
                        OptionConst::NUM_CONNECTIONS => 10,
                    ],
                    OptionConst::MAX_RUNNING_SECONDS => 0,
                    OptionConst::IDLE_TIMEOUT => 30,
                    OptionConst::WAITING_LIST_SIZE => 5,
                    'port' => env('DB_PORT', '3306'),
                    'database' => env('DB_DATABASE', 'forge'),
                    'charset' => 'utf8mb4',
                    'collation' => 'utf8mb4_unicode_ci',
                    'strict' => true,
                    'engine' => null
                ]
            );
        });
    }

    private function setupLogger($app)
    {
        tap($app->make('config'), function (Repository $config) {
            $config->set('logging.default', 'stderr');
        });
    }

    protected function stdout(string $message, array $context = [])
    {
        static $out = null;
        if (null === $out) {
            $out = fopen('php://stdout', 'w');
        }
        fwrite($out, $message . PHP_EOL);
    }
}
