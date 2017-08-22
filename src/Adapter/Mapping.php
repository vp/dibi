<?php

namespace UniMapper\Dibi\Adapter;

use UniMapper\Dibi\Date;
use UniMapper\Entity\Filter;
use UniMapper\Entity\Reflection;
use UniMapper\Association;
use UniMapper\Exception\InvalidArgumentException;

class Mapping extends \UniMapper\Adapter\Mapping
{

    public function mapValue(Reflection\Property $property, $value)
    {
        if ($value instanceof \DibiDateTime) {
            return new \DateTime($value);
        }
        return $value;
    }

    public function unmapValue(Reflection\Property $property, $value)
    {
        if ($property->getType() === Reflection\Property::TYPE_DATE) {
            return new Date($value);
        }
        return $value;
    }

    protected function unmapFilterJoinsPropertyWhere(\UniMapper\Mapper $mapper, Reflection $reflection, $name, $item)
    {
        $result = [];
        $properties = explode('.', $name);
        $unmappedName = null;
        $unmapped = [];
        $currentReflection = $reflection;
        $property = null;
        $path = [];
        while ($current = array_shift($properties)) {
            $property = $currentReflection->getProperty($current);
            $unmapped[] = $property->getUnmapped();
            if (count($properties) > 0) {
                $path[] = $property->getName();
                $currentReflection = \UniMapper\Entity\Reflection::load($property->getTypeOption());
            } else {
                $unmappedName = $property->getUnmapped();
            }

        }
        $unmappedName = implode('_', $path) . '.' . $unmappedName;
        $mapper->unmapFilterProperty($property, $unmappedName, $item, $result);
        return $result;
    }

    protected function unmapFilterJoinPropertyJoin(
        Reflection\Property $assocProperty,
        \UniMapper\Entity\Reflection\Property\Option\Assoc $association
    ) {
        $alias = $assocProperty->getUnmapped();
        $joins = [];
        $table = $assocProperty->getReflection()->getAdapterResource();

        $targetResource = $association->getTargetReflection()->getAdapterResource();
        $targetPrimaryKey = $association->getTargetReflection()->getPrimaryProperty()->getUnmapped();
        $sourcePrimaryKey = $association->getSourceReflection()->getPrimaryProperty()->getUnmapped();

        switch ($association->getType()) {
            case "m:n":
            case "m>n":
            case "m<n":
                list($joinKey, $joinResource, $referencingKey) = $association->getBy();
                $joinResourceAlias = $alias . '_' . $joinResource;
                $joins[] = "[{$joinResource}] AS [{$joinResourceAlias}] ON  [{$joinResourceAlias}].[{$joinKey}] = [{$table}].[{$sourcePrimaryKey}]";
                $joins[] = "[{$targetResource}] AS [{$alias}] ON  [{$alias}].[{$targetPrimaryKey}] = [{$joinResourceAlias}].[{$referencingKey}]";
                break;
            case "1:n":
                list($referencedKey) = $association->getBy();
                $joins[] = "[{$targetResource}] AS [{$alias}] ON [{$alias}].[{$referencedKey}] = [{$table}].[{$sourcePrimaryKey}]";
                break;
            case "1:1":
            case "n:1":
                list($referencingKey) = $association->getBy();
                $joins[] = "[{$targetResource}] AS [{$alias}] ON [{$alias}].[{$targetPrimaryKey}] = [{$table}].[{$referencingKey}]";
                break;
        }

        return $joins;
    }

    public function unmapFilterJoins(\UniMapper\Mapper $mapper, Reflection $reflection, array $filter)
    {
        $where = [];
        $joins = [];

        if (Filter::isGroup($filter)) {

            foreach ($filter as $modifier => $item) {
                list ($groupWhere, $groupJoins) =  $this->unmapFilterJoins($mapper, $reflection, $item);
                $joins = array_merge($joins, $groupJoins);
                $where = array_merge($where, [$modifier => $groupWhere]);
            }
        } else {

            $created = [];
            foreach ($filter as $name => $item) {
                if ($name === Filter::_NATIVE) {
                    continue;
                }

                $assocDelimiterPos = strpos($name, '.');
                if ($assocDelimiterPos !== false) {
                    // get first property
                    $assocPropertyName = substr($name, 0, strpos($name, '.'));
                    $assocProperty = $reflection->getProperty($assocPropertyName);

                    /** @var \UniMapper\Entity\Reflection\Property\Option\Assoc $assoc */
                    $assoc = $assocProperty->getOption(Reflection\Property\Option\Assoc::KEY);

                    if ($assoc->isCustom()) {
                        $association = Association::create($assoc);
                        if (method_exists($association, 'unmapFilterJoin')) {
                            // get it
                            list ($customWhere, $customJoin) = $association->unmapFilterJoin(
                                $mapper,
                                $reflection,
                                $name,
                                $item,
                                $filter
                            );

                            // where
                            if ($customWhere) {
                                $where = array_merge(
                                    $where,
                                    $customWhere
                                );
                            }

                            // joins
                            if ($customJoin) {
                                $joins = array_merge(
                                    $joins,
                                    $customJoin
                                );
                            }

                        } else {
                            throw new InvalidArgumentException(
                                "Custom association not support nested filters " . $name . "!"
                            );
                        }
                    } else {

                        // where
                        $where = array_merge(
                            $where,
                            $this->unmapFilterJoinsPropertyWhere($mapper, $reflection, $name, $item)
                        );

                        // joins
                        $alias = $assocProperty->getUnmapped();
                        if (!isset($created[$alias])) {
                            $joins = array_merge(
                                $joins,
                                $this->unmapFilterJoinPropertyJoin($assocProperty, $assoc)
                            );
                            $created[$alias] = true;
                        }
                    }
                }
            }
        }

        return [$where, $joins];
    }

}