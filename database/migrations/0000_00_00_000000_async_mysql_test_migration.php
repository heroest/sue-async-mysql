<?php

use Sue\Async\Mysql\Tests\Data\Cat;
use Sue\Async\Mysql\Tests\Data\User;
use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\DB;

class AsyncMysqlTestMigration extends Migration
{
    protected $connection = 'mysql';

    public function up()
    {
        Schema::dropIfExists('users');
        Schema::create('users', function (Blueprint $table) {
                $table->increments('id');
                $table->string('name', 32);
                $table->integer('age')->default(0);
            });
        DB::table('users')->insert(User::data());

        Schema::dropIfExists('cats');
        Schema::create('cats', function (Blueprint $table) {
                $table->increments('id');
                $table->integer('user_id')->unsigned()->default(0);
                $table->string('name', 32);
                $table->integer('age')->default(0);
            });
        DB::table('cats')->insert(Cat::data());

        Schema::dropIfExists('dogs');
        Schema::create('dogs', function (Blueprint $table) {
            $table->increments('id');
            $table->integer('user_id')->unsigned()->default(0);
            $table->string('name', 32);
            $table->json('json');
        });
    }

    public function down()
    {
        Schema::dropIfExists('users');
        Schema::dropIfExists('cats');
        Schema::dropIfExists('dogs');
    }
}
