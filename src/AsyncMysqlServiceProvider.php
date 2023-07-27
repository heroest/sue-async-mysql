<?php

namespace Sue\Async\Mysql;

use Psr\Log\LoggerInterface;
use Illuminate\Support\ServiceProvider;

class AsyncMysqlServiceProvider extends ServiceProvider
{
    /** @inheritDoc */
    public function register()
    {
        $this->app->resolving('db', function ($db) {
            $db->extend('async-mysql', function ($config, $name) {
                $config['name'] = $name;
                $logger = $this->app->make(LoggerInterface::class);
                return new Connection($config, $logger);
            });
        });
    }
}