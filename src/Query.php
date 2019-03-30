<?php

namespace UniMapper\Dibi;

use UniMapper\Entity\Filter;

class Query implements \UniMapper\Adapter\IQuery, \UniMapper\Adapter\IQueryWithJoins
{

    /** @var \UniMapper\Entity\Reflection\Property\Option\Assoc[] */
    public $associations = [];

    /** @var array */
    public $associationsFilters = [];

    /** @var \DibiFluent */
    public $fluent;

    public $resultCallback;

    /** @var array $modificators Dibi modificators */
    private $modificators = [
        "boolean" => "%b",
        "integer" => "%i",
        "string" => "%s",
        "NULL" => "%sN",
        "DateTime" => "%t",
        "UniMapper\Dibi\Date" => "%d",
        "array" => "%in",
        "double" => "%f"
    ];

    /** @var  string */
    private $table;

    public function __construct(\DibiFluent $fluent, $table = null)
    {
        $this->fluent = $fluent;
        $this->table = $table;
    }

    public function getModificators()
    {
        return $this->modificators;
    }

    public function setFilter(array $filter)
    {
        if ($filter) {
            $this->fluent->where("%and", $this->convertFilter($filter));
        }
    }

    public function setAssociations(array $associations, array $associationsFilters = [])
    {
        $this->associations += $associations;
        $this->associationsFilters += $associationsFilters;
    }

    public function convertFilter(array $filter)
    {
        $result = [];

        if (Filter::isGroup($filter)) {
            // Filter group

            foreach ($filter as $modifier => $item) {
                $result[] = [
                    $modifier === Filter::_OR ? "%or" : "%and",
                    $this->convertFilter($item)
                ];
            }
        } else {
            // Filter item
            
            foreach ($filter as $name => $item) {
                if ($name === Filter::_NATIVE) {
                    if (is_array($item)) {
                        unset($item['joins']);
                        $result[] = ['%and', $item];
                    } else {
                        $result[] = $item;
                    }
                    continue;
                }
                foreach ($item as $operator => $value) {

                    // Convert data type definition to modificator
                    $type = gettype($value);
                    if ($type === "object") {
                        $type = get_class($value);
                    }
                    if (!isset($this->modificators[$type])) {
                        throw new \Exception("Unsupported value type " . $type . " given!");
                    }
                    $modificator = $this->modificators[$type];

                    if ($operator === Filter::START) {

                        $operator = "LIKE";
                        $modificator = "%like~";
                    } elseif ($operator === Filter::END) {

                        $operator = "LIKE";
                        $modificator = "%~like";
                    } elseif ($operator === Filter::CONTAIN) {

                        $operator = "LIKE";
                        $modificator = "%~like~";
                    }

                    if ($modificator === "%in") {

                        if ($operator === Filter::EQUAL) {
                            $operator = "IN";
                        } elseif ($operator === Filter::NOT) {
                            $operator = "NOT IN";
                        }
                    } elseif (in_array($modificator, ["%sN", "%b"], true)) {

                        if ($operator === Filter::EQUAL) {
                            $operator = "IS";
                        } elseif ($operator === Filter::NOT) {
                            $operator = "IS NOT";
                        }
                    }

                    if ($operator === Filter::NOT) {
                        $operator = "!=";
                    }

                    if ($this->table && strpos($name, '.') === false) {
                        $name =  $this->table . '.' . $name;
                    }

                    $result[] = [
                        "%n %sql " . $modificator,
                        $name,
                        $operator,
                        $value
                    ];
                }
            }
        }

        return $result;
    }

    public function getRaw()
    {
        return (string) $this->fluent;
    }

    public function setJoins(array $joins)
    {
        foreach ($joins as $join) {
            $this->fluent->innerJoin($join);
        }
    }

    public function addOption($name, $value)
    {
        throw new \UniMapper\Exception\QueryException('Not supported in dibi query');
    }


}