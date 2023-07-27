sue\async-mysql
====================
提供基于`Illuminate\Database`包的异步执行SQL的组件，具体使用可以参考：[使用手册](https://learnku.com/docs/laravel/8.x/eloquent/9400)

## 需求
> php >= 5.6.4
> ext-pdo
> ext-mysqli

## 安装方法
```bash
$ composer require sue/async-mysql
```
强烈推荐配合[sue\coroutine](https://packagist.org/packages/sue/coroutine)协程组件来避免[回调地狱](https://zhuanlan.zhihu.com/p/326902537)

**内容列表**
* [注意事项](#注意事项)
* [Quickstart example](#quickstart-example)
* [使用方式]
    * [laravel框架](#laravel框架)
    * [其他框架](#其他框架)
* [功能列表]
    * [数据库事务](#数据库事务)
    * [读写分离](#读写分离)
    * [设置SQL超时时间](#SQL超时)
    * [unprepared方法](#unprepared)
    * [连接资源回收](#连接资源回收)
    * [ping](#ping)
* [Tests](#tests)
* [License](#license)

#### 注意事项
- `Illuminate\Database\Eloquent`以及`Illuminate\Database\Migrations`暂时不支持
- 重写了Illuminate\Database\Connection类，DB::pretend()方法暂时也无法使用

#### Quickstart example

```php
use Illuminate\Support\Facades\DB;

use function Sue\EventLoop\loop;
use function Sue\Coroutine\co;

co(function () {
    $connection = DB::connection('async-mysql');
    //以下3条SQL将并行执行
    $users = yield [
        $connection->table('users')->where('name', 'foo')->first(),
        $connection->table('users')->where('name', 'bar')->first(),
        $connection->table('users')->where('name', 'baz')->first()
    ];
    echo $users[0]->name; //expected value is 'foo'
    echo $users[1]->name; //expected value is 'bar'
    echo $users[2]->name; //expected value is 'baz'
});
```

#### laravel框架
- 在`config/app.php` 中引入service-provider `Sue\Async\Mysql\AsyncMysqlServiceProvider`
- 在`config/database.php`中的`connections`直接复制现有的配置，然后修改driver为`async-mysql`

```php
 [
    'driver' => 'async-mysql',
    'write' => [
        'host' => ['localhost1', 'localhost2'],
        'username' => 'root',
        'password' => 'root',
        /** 可选配置
        OptionConst::NUM_CONNECTIONS => 5, //配置链接数为5
        */
    ],
    'read' => [
        'host' => ['localhost1', 'localhost2'],
        'username' => 'root',
        'password' => 'root',
        /** 可选配置
        OptionConst::NUM_CONNECTIONS => 10, //配置连接数为5
        */
    ],
    /**
     * 可选配置
    OptionConst::MAX_RUNNING_SECONDS => 30, //SQL最大运行时间
    OptionConst::IDLE_TIMEOUT => 30, //连接被回收前闲置的最大时间限制
    OptionConst::WAITING_LIST_SIZE => 1000, //待执行SQL的等待列表大小
    **/
    'port' => env('DB_PORT', '3306'),
    'database' => env('DB_DATABASE', 'forge'),
    'charset' => 'utf8mb4',
    'collation' => 'utf8mb4_unicode_ci',
    'strict' => true,
    'engine' => null
];
```
- 使用方法

```php
use Illuminate\Support\Facades\DB;

use function Sue\EventLoop\loop();

DB::connection('async-mysql')
    ->table('users')
    ->where('username', 'foo')
    ->first()
    ->then(function ($user) {
        echo "User " . $user->username . "\n";
    }, function ($error) {
        echo $error . "\n";
    });
loop()->run();
```

#### 其他框架
- 先在合适的地方添加以下代码， 比方说`index.php`等入口文件

```php
$db = new \Illuminate\Database\Capsule\Manager;
$db->addConnection([
    'driver' => 'async-mysql',
    'write' => [
        'host' => ['localhost'],
        'username' => 'root',
        'password' => 'root',
        /** 可选配置
        OptionConst::NUM_CONNECTIONS => 5, //配置链接数为5
        */
    ],
    'read' => [
        'host' => ['localhost'],
        'username' => 'root',
        'password' => 'root',
        /** 可选配置
        OptionConst::NUM_CONNECTIONS => 10, //配置连接数为5
        */
    ],
    /**
     * 可选配置
    OptionConst::MAX_RUNNING_SECONDS => 30, //SQL最大运行时间
    OptionConst::IDLE_TIMEOUT => 30, //连接被回收前闲置的最大时间限制
    OptionConst::WAITING_LIST_SIZE => 1000, //待执行SQL的等待列表大小
    **/
    'port' => 3306,
    'database' => 'main',
    'charset' => 'utf8mb4',
    'collation' => 'utf8mb4_unicode_ci',
    'strict' => true,
    'engine' => null
], 'async-mysql');
$manager = $db->getDatabaseManager();
$logger = new PsrLogger(); //这里可以初始化一个 \Psr\Log\LoggerInterface的实例，没有的话传null
$manager->extend('async-mysql', function ($config, $name, $logger) {
    $config['name'] = $name;
    return new Connection($config, $logger);
});
$db->setAsGlobal(); //设置全局可用
```

- 使用方法

```php
use Illuminate\Database\Capsule\Manager as DB;
use Sue\EventLoop\loop;

DB::connection('async-mysql')
    ->table('users')
    ->where('username', 'foo')
    ->first()
    ->then(function ($user) {
        echo "User " . $user->username . "\n";
    }, function ($error) {
        echo $error . "\n";
    });
loop()->run();
```

#### 数据库事务
`sue\async\mysql`由于采用了连接池，数据库事务跟链接绑定，所以要在事务中执行SQL的话需要使用指定的链接
*** 暂时不支持事务嵌套以及savepoint ***

```php
use function Sue\Coroutine\co;
use function Sue\EventLoop\loop;

co(function () {
    $connection = DB::connection('async-mysql')
    $transaction = yield $connection->beginTransaction();
    try {
        $num_updated = yield $connection->table('users')
            ->setTransaction($transaction)
            ->where('name', 'foo')
            ->update(['age' => 18]);
        $bool = yield $connection->commit($transaction);
    } catch (Exception $e) {
        echo "some error: " .  $e . "\n";
        yield $connection->rollback($transaction);
    }
});

loop()->run();
```

#### 读写分离
`sue/async-mysql`会自行判断使用主库还是从库，也可以使用方法`setUseWrite()`方法来指定强制用主库执行SQL

```php
DB::connection('async-mysql')
    ->table('users')
    ->setUseWrite() //强制读主库
    ->where('name', 'foo')
    ->first()
    ->then(function ($user) {
        echo "name: " . $user->name;
    }, function ($error) {
        echo $error;
    });

loop()->run();
```

#### SQL超时
[SQL超时时间] = [SQL执行时间] + [等待列表时等待数据库连接分配时间]
设置SQL执行超时时间有两种方式
- 在`config`中配置全局的SQL最大执行时间

```php
use Sue\Async\Mysql\OptionConst;

$config = [
    'async-mysql' => [
        'driver' => 'async-mysql',
        'read' => [],
        'write' => [],
        'port' => 3306,
        OptionConst::MAX_RUNNING_SECONDS => 30, //最多运行30秒
    ]
];
```

- 针对单个SQL设置超时时间

```php
use React\Promise\Timer\TimeoutException;

DB::connection('async-mysql')
    ->table('users')
    ->setTimeout(3) //超时时间3秒
    ->selectRaw('SLEEP(5)') //让mysql sleep5秒后再返回结果
    ->first()
    ->then(function ($user) {
        //wont be here
    }, function (TimeoutException $e) {
        echo "timeout exception";
    });

```

#### unprepared
`unprepared()`方法现在接受一个额外参数`QueryOption`, 可以设置`超时时间` / `主从配置` / `数据库事务`配置

```php
use Sue\Async\Mysql\Query\QueryOption;

use function Sue\EventLoop\loop;
use function Sue\Coroutine\co;

//设置超时时间
$option = new QueryOption();
$option->setTimeout(2); //设置SQL 2秒超时
DB::connection('async-mysql')->unprepared("select * from users", $option);

//设置强制读主
$option = new QueryOption();
$option->setUseWrite();
DB::connection('async-mysql')->unprepared("select * from users", $option);

//设置事务
co(function () {
    $connection = DB::connection('async-mysql');
    $transaction = yield $connection->startTransaction();
    $option = new QueryOption();
    $option->setTransaction($transaction);
    $connection->unprepared("select * from users", $option);
    yield $connection->commit($transaction);
});

loop()->run();
```

#### 连接资源回收
`async-mysql`默认不回收资源，原因如下
- php-fpm方式中没必要
- 在常驻进程中加定时器可能导致`loop()->run()`无法自动退出
- 如有需要可以用以下方式手动开启

```php
$connection = DB::connection('async-mysql');
$seconds_delay = 3;
$connection->setPipeRecycleDebounceSeconds($seconds_delay);
//SQL完成后防抖3秒后尝试回收闲置时间超过IDLE_TIMEOUT的处于idle状态的连接
```

#### ping
`async-mysql`默认不开启ping功能，原因跟[连接资源回收](#连接资源回收)一致，如有需要可以手动开启

```php
$connection = DB::connection('async-mysql');
$seconds_delay = 3;
$connection->setPipePingDebounceSeconds($seconds_delay);
//SQL请求时防抖3秒后尝试ping所有非关闭的连接，如果ping失败的话会尝试关闭处于idle状态的连接
```

#### tests
- 先拉取项目
```bash
$ git clone http://gitlab.sue.com/PHP_Component/async-mysql.git
```

- 然后再phpunit.xml.dist里配置数据库连接
```xml
<server name="DB_CONNECTION" value="mysql"/>
<server name="DB_HOST" value="127.0.0.1"/>
<server name="DB_DATABASE" value="main"/>
<server name="DB_USERNAME" value="root"/>
<server name="DB_PASSWORD" value="root"/>
```

- 最后执行单元测试
```bash
$ ./vendor/bin/testbench package:test
```

#### License

The MIT License (MIT)

Copyright (c) 2023 Donghai Zhang

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished
to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.