<?php

namespace Sue\Async\Mysql\Tests\Data;

final class User
{
    public static function data(): array
    {
        return [
            ['name' => 'foo', 'age' => 10],
            ['name' => 'bar', 'age' => 20],
            ['name' => 'zhangsan', 'age' => 20],
            ['name' => 'lisi', 'age' => 30],
            ['name' => "wang'wu", "age" => 5],
            ['name' => 'xihai?', "age" => 13],
        ];
    }
}