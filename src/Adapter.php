<?php

namespace UniMapper\Dibi;

use UniMapper\Adapter\IQuery,
    UniMapper\Exception\InvalidArgumentException,
    UniMapper\Association;

class Adapter extends \UniMapper\Adapter
{

    /** @var \DibiConnection $connection Connection to database */
    protected $connection;

    public function __construct(array $config)
    {
        parent::__construct(new Adapter\Mapping);
        $this->connection = new \DibiConnection($config);
    }

    /**
     * Return's dibi connection
     *
     * @return \DibiConnection
     */
    public function getConnection()
    {
        return $this->connection;
    }

    public function query()
    {
        return call_user_func_array([$this->connection, "query"], func_get_args());
    }

    public function createDelete($table)
    {
        $query = new Query($this->connection->delete($table), $table);
        $query->resultCallback = function (Query $query) {

            $query->fluent->execute();
            return $this->connection->getAffectedRows();
        };
        return $query;
    }

    public function createDeleteOne($table, $column, $value)
    {
        $query = new Query(
            $this->connection->delete($table)->where("%n = %s", $column, $value)
        );
        $query->resultCallback = function (Query $query) {

            $query->fluent->execute();
            return $this->connection->getAffectedRows() === 0 ? false : true;
        };
        return $query;
    }

    public function createSelectOne($table, $column, $value)
    {
        $query = new Query(
            $this->connection->select("*")
                ->from("%n", $table)
                ->where("%n = %s", $column, $value), // @todo
            $table
        );

        $query->resultCallback = function (Query $query) use ($value) {

            $result = $query->fluent->fetch();
            if (!$result) {
                return false;
            }

            // Associations
            foreach ($query->associations as $association) {

                $value = $result->{$association->getKey()};
                if (empty($value)) {
                    continue;
                }

                if ($association instanceof Association\OneToMany) {
                    $associated = $this->_oneToMany($association, [$value]);
                } elseif ($association instanceof Association\ManyToOne) {
                    $associated = $this->_manyToOne($association, [$value]);
                } elseif ($association instanceof Association\ManyToMany) {
                    $associated = $this->_manyToMany($association, [$value]);
                } elseif ($association instanceof Association\OneToOne) {
                    $associated = $this->_oneToOne($association, [$value]);
                } else {
                    throw new InvalidArgumentException(
                        "Unsupported association " . get_class($association) . "!",
                        $association
                    );
                }

                if (isset($associated[$value])) {
                    $result[$association->getPropertyName()] = $associated[$value];
                }
            }

            return $result;
        };

        return $query;
    }

    public function createSelect($table, array $selection = [], array $orderBy = [], $limit = 0, $offset = 0)
    {
        if (empty($selection)) {
            $selection = "*";
        } else {
            $selection = "[$table].[" . implode("],[$table].[", $selection) . "]";
        }

        $query = new Query($this->connection->select($selection)->from("%n", $table), $table);

        if (!empty($limit)) {
            $query->fluent->limit("%i", $limit);
        }

        if (!empty($offset)) {
            $query->fluent->offset("%i", $offset);
        }

        if ($orderBy) {
            foreach ($orderBy as $name => $direction) {
                $query->fluent->orderBy($name)->{$direction}();
            }
        }

        $query->resultCallback = function (Query $query) {

            // Select
            foreach ($query->associations as $association) {

                if ($association instanceof Association\ManyToOne) {
                    $query->fluent->select($association->getReferencingKey()); // @todo maybe useless
                }
            }

            $result = $query->fluent->fetchAll(null);
            if (count($result) === 0) {
                return false;
            }

            // Associations
            foreach ($query->associations as $association) {

                $primaryKeys = [];
                foreach ($result as $row) {

                    if (!empty($row->{$association->getKey()})
                        && !in_array($row, $primaryKeys, true)
                    ) {
                        $primaryKeys[] = $row->{$association->getKey()};
                    }
                }

                if (empty($primaryKeys)) {
                    continue;
                }

                if ($association instanceof Association\OneToMany) {
                    $associated = $this->_oneToMany($association, $primaryKeys);
                } elseif ($association instanceof Association\ManyToOne) {
                    $associated = $this->_manyToOne($association, $primaryKeys);
                } elseif ($association instanceof Association\ManyToMany) {
                    $associated = $this->_manyToMany($association, $primaryKeys);
                } elseif ($association instanceof Association\OneToOne) {
                    $associated = $this->_oneToOne($association, $primaryKeys);
                } else {
                    throw new InvalidArgumentException(
                        "Unsupported association " . get_class($association) . "!",
                        $association
                    );
                }

                foreach ($result as $index => $item) {

                    if (isset($associated[$item->{$association->getKey()}])) {
                        $result[$index][$association->getPropertyName()] = $associated[$item->{$association->getKey()}];
                    }
                }
            }

            return $result;
        };

        return $query;
    }

    private function _oneToMany(Association\OneToMany $association, array $primaryKeys)
    {
        return $this->connection->select("*")
            ->from("%n", $association->getTargetResource())
            ->where("%n IN %l", $association->getReferencedKey(), $primaryKeys)
            ->fetchAssoc($association->getReferencedKey() . ",#");
    }

    private function _oneToOne(Association\OneToOne $association, array $primaryKeys)
    {
        return $this->connection->select("*")
            ->from("%n", $association->getTargetResource())
            ->where("%n IN %l", $association->getTargetPrimaryKey(), $primaryKeys)
            ->fetchAssoc($association->getTargetPrimaryKey() . ",#");
    }

    private function _manyToOne(Association\ManyToOne $association, array $primaryKeys)
    {
        $primaryColumn = $association->getTargetReflection()
            ->getPrimaryProperty()
            ->getName(true);

        return $this->connection->select("*")
            ->from("%n", $association->getTargetResource())
            ->where("%n IN %l", $primaryColumn, $primaryKeys)
            ->fetchAssoc($primaryColumn);
    }

    private function _manyToMany(Association\ManyToMany $association, array $primaryKeys)
    {
        $joinResult = $this->connection->select("%n,%n", $association->getJoinKey(), $association->getReferencingKey())
            ->from("%n", $association->getJoinResource())
            ->where("%n IN %l", $association->getJoinKey(), $primaryKeys)
            ->fetchAssoc($association->getReferencingKey() . "," . $association->getJoinKey());

        if (empty($joinResult)) {
            return [];
        }

        $targetResult = $this->connection->select("*")
            ->from("%n", $association->getTargetResource())
            ->where("%n IN %l", $association->getTargetPrimaryKey(), array_keys($joinResult))
            ->fetchAssoc($association->getTargetPrimaryKey());

        $result = [];
        foreach ($joinResult as $targetKey => $join) {

            foreach ($join as $originKey => $data) {
                $result[$originKey][] = $targetResult[$targetKey];
            }
        }

        return $result;
    }

    public function createModifyManyToMany(
        Association\ManyToMany $association,
        $primaryValue,
        array $refKeys,
        $action = self::ASSOC_ADD
    ) {
        if ($action === self::ASSOC_ADD) {

            $fluent = $this->connection->insert(
                $association->getJoinResource(),
                [
                    $association->getJoinKey() => array_fill(0, count($refKeys), $primaryValue),
                    $association->getReferencingKey() => $refKeys
                ]
            );
        } else {

            $fluent = $this->connection->delete($association->getJoinResource())
                ->where("%n = %s", $association->getJoinKey(), $primaryValue) // @todo %s modificator
                ->and("%n IN %l", $association->getReferencingKey(), $refKeys);
        }

        $query = new Query($fluent, $table);
        $query->resultCallback = function (Query $query) {
            return $query->fluent->execute();
        };

        return $query;
    }

    public function createCount($table)
    {
        $query = new Query($this->connection->select("*")->from("%n", $table), $table);
        $query->resultCallback = function (Query $query) {
            return $query->fluent->count();
        };
        return $query;
    }

    public function createInsert($table, array $values, $primaryName = null)
    {
        $query = new Query($this->connection->insert($table, $values), $table);
        $query->resultCallback = function (Query $query) {

            $query->fluent->execute();
            return $this->connection->getInsertId();
        };
        return $query;
    }

    public function createUpdate($table, array $values)
    {
        $query = new Query($this->connection->update($table, $values), $table);
        $query->resultCallback = function (Query $query) {

            $query->fluent->execute();
            return $this->connection->getAffectedRows();
        };
        return $query;
    }

    public function createUpdateOne($table, $primaryColumn, $primaryValue, array $values)
    {
        $type = is_object($primaryValue) ? get_class($primaryValue) : gettype($primaryValue);

        $query = new Query($this->connection->update($table, $values), $table);
        $query->fluent->where("%n = " . $query->getModificators()[$type], $primaryColumn, $primaryValue);
        $query->resultCallback = function (Query $query) {

            $query->fluent->execute();
            return $this->connection->getAffectedRows() === 0 ? false : true;
        };
        return $query;
    }

    public function onExecute(IQuery $query)
    {
        $callback = $query->resultCallback;
        return $callback($query);
    }

}