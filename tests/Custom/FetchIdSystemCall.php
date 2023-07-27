<?php

namespace Sue\Async\Mysql\Tests\Custom;

use Sue\Coroutine\Coroutine;
use Sue\Coroutine\SystemCall\AbstractSystemCall;

class FetchIdSystemCall extends AbstractSystemCall
{
    public function run(Coroutine $coroutine)
    {
        return spl_object_hash($coroutine);
    }
}