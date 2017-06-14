<?php

use Tester\Assert;

require __DIR__ . '/../bootstrap.php';

/**
 * @dataProvider common/db.ini
 */
class AdapterTest extends Tester\TestCase
{

    /** @var \UniMapper\Dibi\Adapter $adapter */
    private $adapter;

    public function __construct(array $config)
    {
        $this->adapter = new UniMapper\Dibi\Adapter($config);
    }

    public function testCreateDelete()
    {
        Assert::same(
            "DELETE FROM [table]",
            $this->adapter->createDelete("table")->getRaw()
        );
    }

    public function testCreateDeleteOne()
    {
        Assert::same(
            "DELETE FROM [table] WHERE [id] = '1'",
            $this->adapter->createDeleteOne("table", "id", 1)->getRaw()
        );
    }

    public function testCreateSelectOneToOne()
    {
        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 1, 'foo_text' => 'foo', 'bar_id' => 1], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar', ['bar_id' => 1, 'bar_text' => 'bar'], 'foo_id')
        );

        $query = $this->adapter->createSelect(
            "foo",
            ["id" => "foo_id", "text" => "foo_text","bar" => ["id" => "bar_id", "text" => "bar_text"]],
            [],
            null,
            null
        );

        Assert::same(
            "SELECT [foo].[foo_id],[foo].[foo_text] FROM [foo]",
            $query->getRaw()
        );

        $result = Foo::query()->select()->associate(['bar'])->run($this->createConnection());

        Assert::equal(
            [
                [
                    'id' => 1,
                    'text' => 'foo',
                    'bar' => ['id' => 1, 'text' => 'bar', 'foo' => NULL, 'foos' => NULL],
                    'bars' => null
                ],
            ],
            $result->toArray(true)
        );

        $this->adapter->execute($this->adapter->createDeleteOne("foo", "foo_id", 1));
        $this->adapter->execute($this->adapter->createDeleteOne("bar", "bar_id", 1));
    }

    public function testCreateSelectOneOneToOne()
    {
        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 2, 'foo_text' => 'foo', 'bar_id' => 2], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar', ['bar_id' => 2, 'bar_text' => 'bar'], 'foo_id')
        );

        $query = $this->adapter->createSelectOne(
            "foo",
            "foo_id",
            2,
            ["id" => "foo_id", "text" => "foo_text","bar" => ["id" => "bar_id", "text" => "bar_text"]]
        );

        Assert::same(
            "SELECT [foo].[foo_id],[foo].[foo_text] FROM [foo] WHERE [foo_id] = '2'",
            $query->getRaw()
        );

        $result = Foo::query()->selectOne(2, ['id','text', 'bar' => ['id', 'text']])->associate(['bar'])->run($this->createConnection());

        \UniMapper\Entity\Iterator::$ITERATE_OPTIONS['defined'] = false;

        Assert::equal(
            [
                'id' => 2,
                'text' => 'foo',
                'bar' => ['id' => 2, 'text' => 'bar'],
            ],
            $result->toArray(true)
        );

        $this->adapter->execute($this->adapter->createDeleteOne("foo", "foo_id", 2));
        $this->adapter->execute($this->adapter->createDeleteOne("bar", "bar_id", 2));
    }

    protected function createConnection()
    {
        $connection = new \UniMapper\Connection(new \UniMapper\Mapper(), null);
        $connection->registerAdapter('Dibi', $this->adapter);
        return $connection;
    }

    public function testCreateSelectManyToOne()
    {
        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 1, 'foo_text' => 'foo', 'bar_id' => 1], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar', ['bar_id' => 1, 'bar_text' => 'bar'], 'foo_id')
        );

        $result = Bar::query()->select()->associate(['foo'])->run($this->createConnection());

        \UniMapper\Entity\Iterator::$ITERATE_OPTIONS['defined'] = true;

        Assert::equal(
            [
                [
                    'id' => 1,
                    'text' => 'bar',
                    'foo' => ['id' => 1, 'text' => 'foo', 'bar' => NULL, 'bars' => NULL],
                    'foos' => NULL
                ],
            ],
            $result->toArray(true)
        );

        $this->adapter->execute($this->adapter->createDeleteOne("foo", "foo_id", 1));
        $this->adapter->execute($this->adapter->createDeleteOne("bar", "bar_id", 1));
    }

    public function testCreateSelectOneManyToOne()
    {
        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 1, 'foo_text' => 'foo', 'bar_id' => 1], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar', ['bar_id' => 1, 'bar_text' => 'bar'], 'foo_id')
        );

        $result = Bar::query()->selectOne(1, ['id', 'text', 'foo'])->associate(['foo'])->run($this->createConnection());

        \UniMapper\Entity\Iterator::$ITERATE_OPTIONS['defined'] = false;

        Assert::equal(
            [
                'id' => 1,
                'text' => 'bar',
                'foo' => ['id' => 1, 'text' => 'foo'],
            ],
            $result->toArray(true)
        );

        $this->adapter->execute($this->adapter->createDeleteOne("foo", "foo_id", 1));
        $this->adapter->execute($this->adapter->createDeleteOne("bar", "bar_id", 1));
    }

    public function testCreateSelectOneToMany()
    {
        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 1, 'foo_text' => 'foo', 'bar_id' => 1], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar', ['bar_id' => 1, 'bar_text' => 'bar'], 'foo_id')
        );

        $result = Foo::query()->select()->associate(['bars'])->run($this->createConnection());

        \UniMapper\Entity\Iterator::$ITERATE_OPTIONS['defined'] = true;

        Assert::equal(
            [
                [
                    'id' => 1,
                    'text' => 'foo',
                    'bar' => NULL,
                    'bars' => [['id' => 1, 'text' => 'bar', 'foo' => NULL, 'foos' => NULL]],
                ],
            ],
            $result->toArray(true)
        );

        $this->adapter->execute($this->adapter->createDeleteOne("foo", "foo_id", 1));
        $this->adapter->execute($this->adapter->createDeleteOne("bar", "bar_id", 1));
    }

    public function testCreateSelectOneOneToMany()
    {
        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 1, 'foo_text' => 'foo', 'bar_id' => 1], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar', ['bar_id' => 1, 'bar_text' => 'bar'], 'foo_id')
        );

        $result = Foo::query()->selectOne(1, ['id', 'text', 'bars'])->associate(['bars'])->run($this->createConnection());

        \UniMapper\Entity\Iterator::$ITERATE_OPTIONS['defined'] = false;

        Assert::equal(
            [
                'id' => 1,
                'text' => 'foo',
                'bars' => [['id' => 1, 'text' => 'bar']],
            ],
            $result->toArray(true)
        );

        $this->adapter->execute($this->adapter->createDeleteOne("foo", "foo_id", 1));
        $this->adapter->execute($this->adapter->createDeleteOne("bar", "bar_id", 1));
    }

    public function testCreateSelectManyToMany()
    {
        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 1, 'foo_text' => 'foo'], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 2, 'foo_text' => 'foo2'], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar', ['bar_id' => 1, 'bar_text' => 'bar'], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar_foo', ['bar_id' => 1, 'foo_id' => 1], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar_foo', ['bar_id' => 1, 'foo_id' => 2], 'foo_id')
        );

        $result = Bar::query()->select()->associate(['foos'])->run($this->createConnection());

        \UniMapper\Entity\Iterator::$ITERATE_OPTIONS['defined'] = true;

        Assert::equal(
            [
                [
                    'id' => 1,
                    'text' => 'bar',
                    'foo' => NULL,
                    'foos' => [
                        ['id' => 1, 'text' => 'foo', 'bar' => NULL, 'bars' => NULL],
                        ['id' => 2, 'text' => 'foo2', 'bar' => NULL, 'bars' => NULL],
                    ],
                ],
            ],
            $result->toArray(true)
        );

        $this->adapter->execute($this->adapter->createDelete("foo"));
        $this->adapter->execute($this->adapter->createDelete("bar"));
        $this->adapter->execute($this->adapter->createDelete("bar_foo"));
    }

    public function testCreateSelectOneManyToMany()
    {
        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 1, 'foo_text' => 'foo'], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('foo', ['foo_id' => 2, 'foo_text' => 'foo2'], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar', ['bar_id' => 1, 'bar_text' => 'bar'], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar_foo', ['bar_id' => 1, 'foo_id' => 1], 'foo_id')
        );

        $this->adapter->execute(
            $this->adapter->createInsert('bar_foo', ['bar_id' => 1, 'foo_id' => 2], 'foo_id')
        );

        $result = Bar::query()->selectOne(1, ['id', 'text'])->associate(['foos'])->run($this->createConnection());

        \UniMapper\Entity\Iterator::$ITERATE_OPTIONS['defined'] = false;

        Assert::equal(
            [
                'id' => 1,
                'text' => 'bar',
                'foos' => [
                    ['id' => 1, 'text' => 'foo'],
                    ['id' => 2, 'text' => 'foo2'],
                ],
            ],
            $result->toArray(true)
        );

        $this->adapter->execute($this->adapter->createDelete("foo"));
        $this->adapter->execute($this->adapter->createDelete("bar"));
        $this->adapter->execute($this->adapter->createDelete("bar_foo"));
    }


    protected function setUp()
    {
        parent::setUp();

        $this->adapter->getConnection()->nativeQuery('
        CREATE TABLE IF NOT EXISTS foo (
         foo_id INTEGER PRIMARY KEY,
         bar_id INTEGER,
         foo_text TEXT
        )');

        $this->adapter->getConnection()->nativeQuery('
        CREATE TABLE IF NOT EXISTS bar (
         bar_id INTEGER PRIMARY KEY,        
         bar_text TEXT
        )');

        $this->adapter->getConnection()->nativeQuery('
        CREATE TABLE IF NOT EXISTS bar_foo (
         bar_id INTEGER NOT NULL,        
         foo_id INTEGER NOT NULL
        )');
    }
}

/**
 * @adapter Dibi(foo)
 *
 * @property int    $id                 m:primary m:map-by(foo_id)
 * @property string $text               m:map-by(foo_text)
 * @property Bar  $bar m:assoc(1:1) m:assoc-by(bar_id)
 * @property Bar[]  $bars m:assoc(1:N) m:assoc-by(bar_id)
 */
class Foo extends \UniMapper\Entity {}

/**
 * @adapter Dibi(bar)
 *
 * @property int    $id                 m:primary m:map-by(bar_id)
 * @property string $text               m:map-by(bar_text)
 * @property Foo  $foo m:assoc(N:1)     m:assoc-by(bar_id)
 * @property Foo[] $foos m:assoc(M:N)   m:assoc-by(bar_id|bar_foo|foo_id)
 */
class Bar extends \UniMapper\Entity {}


$testCase = new AdapterTest($config);
$testCase->run();