<?php

namespace Sue\Async\Mysql\Tests\Data;

final class Cat
{
    public static function data(): array
    {
        return [
            ['user_id' => 1, 'name' => 'chino', 'age' => 10],
            ['user_id' => 1, 'name' => 'egg', 'age' => 20],
            ['user_id' => 3, 'name' => 'wangmm', 'age' => 20],
            ['user_id' => 4, 'name' => 'lefay', 'age' => 30],
        ];
    }
}