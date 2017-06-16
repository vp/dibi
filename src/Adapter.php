<?php

namespace UniMapper\Dibi;

use UniMapper\Adapter\IQuery,
    UniMapper\Exception\InvalidArgumentException,
    UniMapper\Association;
use UniMapper\Entity\Reflection\Property\Option\Assoc;

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

    protected function prepareSelection(array $selection = [])
    {
        $associations = [];
        if ($selection) {

            foreach ($selection as $k => $v) {
                if (is_array($v)) {
                    unset($selection[$k]);
                    $associations[$k] = $v;
                }
            }
        }

        return [$selection, $associations];
    }

    protected function associate(Assoc $association, array $referencingKeys, array $targetSelection,  $result)
    {
        switch ($association->getType()) {
            case "m:n":
            case "m>n":
            case "m<n":
            $associated = $this->_manyToMany($association, $referencingKeys, $targetSelection);
            break;
            case "1:n":
            $associated = $this->_oneToMany($association, $referencingKeys, $targetSelection);
            break;
            case "1:1":
                $associated = $this->_oneToOne($association, $referencingKeys, $targetSelection);
                break;
            case "n:1":
                $associated = $this->_manyToOne($association, $referencingKeys, $targetSelection);
                break;
            default:
                throw new InvalidArgumentException(
                    "Unsupported association " . $association->getType() . "!",
                    $association
                );
        }
        return $associated;
    }

    protected function getAssociationReferencingKey(Assoc $option)
    {
        switch ($option->getType()) {
            case "m:n":
            case "m>n":
            case "m<n":
            case "1:n":
                return $option->getSourceReflection()
                    ->getPrimaryProperty()
                    ->getUnmapped();
            case "1:1":
            case "n:1":
                $by = $option->getBy();
                return $by[0];
            default:
                throw new InvalidArgumentException(
                    "Unsupported association " . $option->getType() . "!",
                    $option
                );
        }
    }

    protected function createSelectionString($table, $tableSelection) {
        if (empty($tableSelection)) {
            return "*";
        } else {
            return "[$table].[" . implode("],[$table].[", array_values($tableSelection)) . "]";
        }
    }

    protected function selectReferencingColumnForAssociation(Query $query, $tableSelection, $table) {
        // Select referencing columns, may not be defined in given selection
        // if selection is empty don't care because * is selected
        if ($tableSelection) {
            foreach ($query->associations as $association) {
                $referencingKey = $this->getAssociationReferencingKey($association);
                if (in_array($referencingKey, $tableSelection) === false) {
                    $query->fluent->select("[$table].[$referencingKey]");
                }
            }
        }

    }


    public function createSelectOne($table, $column, $value, $selection = [])
    {
        list($tableSelection, $associationsSelection) = $this->prepareSelection($selection);

        $query = new Query(
            $this->connection->select($this->createSelectionString($table, $tableSelection))
                ->from("%n", $table)
                ->where("%n = %s", $column, $value), // @todo
            $table
        );

        $query->resultCallback = function (Query $query) use ($table, $value, $tableSelection, $associationsSelection) {

            $this->selectReferencingColumnForAssociation(
                $query,
                $tableSelection,
                $table
            );

            $result = $query->fluent->fetch();
            if (!$result) {
                return false;
            }

            // Associations
            foreach ($query->associations as $propertyName => $association) {

                $referencingKey = $this->getAssociationReferencingKey($association);

                $value = $result->{$referencingKey};
                if (empty($value)) {
                    continue;
                }

                $targetSelection = isset($associationsSelection[$propertyName])
                    ? $associationsSelection[$propertyName]
                    : [];

                $associated = $this->associate(
                    $association,
                    [$value],
                    $targetSelection,
                    $result
                );

                if (isset($associated[$value])) {
                    $result[$propertyName] = $associated[$value];
                } else {
                    $result[$propertyName] = $association->getSourceReflection()->getProperty($propertyName)->getType() === \UniMapper\Entity\Reflection\Property::TYPE_COLLECTION
                        ? []
                        : null;
                }
            }

            return $result;
        };

        return $query;
    }

    public function createSelect($table, array $selection = [], array $orderBy = [], $limit = 0, $offset = 0)
    {
        list($tableSelection, $associationsSelection) = $this->prepareSelection($selection);

        $query = new Query($this->connection
            ->select($this->createSelectionString($table, $tableSelection))
            ->from("%n", $table), $table);

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

        $query->resultCallback = function (Query $query) use ($table, $tableSelection, $associationsSelection) {

            $this->selectReferencingColumnForAssociation(
                $query,
                $tableSelection,
                $table
            );

            $result = $query->fluent->fetchAll(null);
            if (count($result) === 0) {
                return false;
            }

            // Associations
            foreach ($query->associations as $propertyName => $association) {

                $referencingKey = $this->getAssociationReferencingKey($association);

                $primaryKeys = [];
                foreach ($result as $row) {

                    if (!empty($row->{$referencingKey})
                        && !in_array($row, $primaryKeys, true)
                    ) {
                        $primaryKeys[] = $row->{$referencingKey};
                    }
                }

                if (empty($primaryKeys)) {
                    continue;
                }

                $targetSelection = isset($associationsSelection[$propertyName])
                    ? $associationsSelection[$propertyName]
                    : [];

                $associated = $this->associate(
                    $association,
                    $primaryKeys,
                    $targetSelection,
                    $result
                );

                foreach ($result as $index => $item) {

                    if (isset($associated[$item->{$referencingKey}])) {
                        $result[$index][$propertyName] = $associated[$item->{$referencingKey}];
                    }
                }
            }

            return $result;
        };

        return $query;
    }

    private function _oneToMany(Assoc $association, array $primaryKeys, array $targetSelection = [])
    {
        list($referencedKey) = $association->getBy();

        return $this->connection->select($targetSelection ? array_values($targetSelection) : "*")
            ->from("%n", $association->getTargetReflection()->getAdapterResource())
            ->where("%n IN %l", $referencedKey, $primaryKeys)
            ->fetchAssoc($referencedKey . ",#");
    }

    private function _oneToOne(Assoc $association, array $primaryKeys, array $targetSelection = [])
    {
        $targetPrimaryKey = $association->getTargetReflection()
            ->getPrimaryProperty()
            ->getUnmapped();

        $result = $this->connection->select($targetSelection ? array_values($targetSelection) : "*")
            ->from("%n", $association->getTargetReflection()->getAdapterResource())
            ->where("%n IN %l", $targetPrimaryKey, $primaryKeys)
            ->fetchAssoc($targetPrimaryKey . ",=");

        return $result;
    }

    private function _manyToOne(Assoc $association, array $primaryKeys, array $targetSelection = [])
    {
        $targetPrimaryKey = $association->getTargetReflection()
            ->getPrimaryProperty()
            ->getUnmapped();

        return $this->connection->select($targetSelection ? array_values($targetSelection) : "*")
            ->from("%n", $association->getTargetReflection()->getAdapterResource())
            ->where("%n IN %l", $targetPrimaryKey, $primaryKeys)
            ->fetchAssoc($targetPrimaryKey);
    }

    private function _manyToMany(Assoc $association, array $primaryKeys, array $targetSelection = [])
    {
        list($joinKey, $joinResource, $referencingKey) = $association->getBy();

        $joinQuery = $this->connection->select("%n,%n", $joinKey, $referencingKey)
            ->from("%n", $joinResource)
            ->where("%n IN %l", $joinKey, $primaryKeys);

        $joinResult = $joinQuery->fetchAssoc($referencingKey . "," . $joinKey);

        if (empty($joinResult)) {
            return [];
        }

        $targetPrimaryKey = $association->getTargetReflection()
            ->getPrimaryProperty()
            ->getUnmapped();

        $targetResult = $this->connection->select($targetSelection ? array_values($targetSelection) : "*")
            ->from("%n", $association->getTargetReflection()->getAdapterResource())
            ->where("%n IN %l", $targetPrimaryKey, array_keys($joinResult))
            ->fetchAssoc($targetPrimaryKey);

        $result = [];
        foreach ($joinResult as $targetKey => $join) {

            foreach ($join as $originKey => $data) {
                $result[$originKey][] = $targetResult[$targetKey];
            }
        }

        return $result;
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

    /**
     * @param string $sourceResource
     * @param string $joinResource
     * @param string $targetResource
     * @param string $joinKey
     * @param string $referencingKey
     * @param mixed  $primaryValue
     * @param array  $keys
     *
     * @return IQuery
     */
    public function createManyToManyAdd($sourceResource, $joinResource, $targetResource, $joinKey, $referencingKey, $primaryValue, array $keys)
    {
        $fluent = $this->connection->insert(
            $joinResource,
            [
                $joinKey => array_fill(0, count($keys), $primaryValue),
                $referencingKey => $keys
            ]
        );

        $query = new Query($fluent, $joinResource);
        $query->resultCallback = function (Query $query) {
            return $query->fluent->execute();
        };

        return $query;
    }

    /**
     * @param string $sourceResource
     * @param string $joinResource
     * @param string $targetResource
     * @param string $joinKey
     * @param string $referencingKey
     * @param mixed  $primaryValue
     * @param array  $keys
     *
     * @return IQuery
     */
    public function createManyToManyRemove($sourceResource, $joinResource, $targetResource, $joinKey, $referencingKey, $primaryValue, array $keys)
    {
        $fluent = $this->connection->delete($joinResource)
            ->where("%n = %s", $joinKey, $primaryValue) // @todo %s modificator
            ->and("%n IN %l", $referencingKey, $keys);

        $query = new Query($fluent, $joinResource);
        $query->resultCallback = function (Query $query) {
            return $query->fluent->execute();
        };

        return $query;
    }
}