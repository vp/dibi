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
        $group = substr($name, 0, strpos($name, '#'));
        $name = self::removeGroup($name);
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
        $unmappedName = ($group ? $group . '_' : '') . implode('_', $path) . '.' . $unmappedName;
        $mapper->unmapFilterProperty($property, $unmappedName, $item, $result);
        return $result;
    }

    protected function unmapFilterJoinPropertyJoin(
        Reflection\Property $assocProperty,
        \UniMapper\Entity\Reflection\Property\Option\Assoc $association,
        $alias
    ) {
        $joins = [];
        $table = $assocProperty->getReflection()->getAdapterResource();

        $targetResource = $association->getTargetReflection()->getAdapterResource();
        $targetPrimaryKey = $association->getTargetReflection()->getPrimaryProperty()->getUnmapped();
        $sourcePrimaryKey = $association->getSourceReflection()->getPrimaryProperty()->getUnmapped();

        switch ($association->getType()) {
            case "m:n":
            case "m>n":
            case "m<n":
                list($joinKey, $joinResource, $joinReferencingKey) = $association->getBy();
                $joinResourceAlias = $alias . '_' . $joinResource;
                $referencedKey = $association->hasParameter('target')
                    ? $association->getParameter('target')
                    : $targetPrimaryKey;

                $referencingKey = $association->hasParameter('source')
                    ? $association->getParameter('source')
                    : $sourcePrimaryKey;

                $joins[] = "[{$joinResource}] AS [{$joinResourceAlias}] ON  [{$joinResourceAlias}].[{$joinKey}] = [{$table}].[{$referencingKey}]";
                $joins[] = "[{$targetResource}] AS [{$alias}] ON  [{$alias}].[{$referencedKey}] = [{$joinResourceAlias}].[{$joinReferencingKey}]";
                break;
            case "1:n":
                list($referencedKey) = $association->getBy();
                $referencingKey = $association->hasParameter('source')
                    ? $association->getParameter('source')
                    : $sourcePrimaryKey;
                $joins[] = "[{$targetResource}] AS [{$alias}] ON [{$alias}].[{$referencedKey}] = [{$table}].[{$referencingKey}]";
                break;
            case "1:1":
            case "n:1":
                list($referencingKey) = $association->getBy();
                $referencedKey = $association->hasParameter('target')
                    ? $association->getParameter('target')
                    : $targetPrimaryKey;

                $joins[] = "[{$targetResource}] AS [{$alias}] ON [{$alias}].[{$referencedKey}] = [{$table}].[{$referencingKey}]";
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
                    if (is_array($item) && isset($item['joins']))
                    {
                        $joins = array_merge($joins, $item['joins']);
                    }
                    continue;
                }

                $assocDelimiterPos = strpos($name, '.');
                if ($assocDelimiterPos !== false) {
                    // get first property
                    $assocPropertyName = substr($name, 0, $assocDelimiterPos);
                    $assocPropertyName = self::removeGroup($assocPropertyName);
                    
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
                        $group = substr($name, 0, strpos($name, '#'));
                        $alias = ($group ? $group . '_' : '') . $assocProperty->getUnmapped();

                        if (!isset($created[$alias])) {
                            $joins = array_merge(
                                $joins,
                                $this->unmapFilterJoinPropertyJoin($assocProperty, $assoc, $alias)
                            );
                            $created[$alias] = true;
                        }
                    }
                }
            }
        }

        return [$where, $joins];
    }


    private static function removeGroup($assocPropertyName)
    {
        $groupHashPos = strpos($assocPropertyName, '#');
        if ($groupHashPos) {
            return substr($assocPropertyName, $groupHashPos + 1);
        }
        
        return $assocPropertyName;
    }
}