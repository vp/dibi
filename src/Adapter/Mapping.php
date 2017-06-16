<?php

namespace UniMapper\Dibi\Adapter;

use UniMapper\Dibi\Date;
use UniMapper\Entity\Filter;
use UniMapper\Entity\Reflection;
use UniMapper\Association;

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

    public function unmapFilterJoins(Reflection $reflection, array $filter)
    {
        $result = [];

        if (Filter::isGroup($filter)) {

            foreach ($filter as $modifier => $item) {
                $result = array_merge($result, $this->unmapFilterJoins($reflection, $item));
            }
        } else {

            $created = [];
            foreach ($filter as $name => $item) {
                $assocDelimiterPos = strpos($name, '.');
                if ($assocDelimiterPos !== false) {
                    $assocPropertyName = substr($name, 0, $assocDelimiterPos);
                    $assocProperty = $reflection->getProperty($assocPropertyName);
                    $alias = $assocProperty->getUnmapped();
                    $table = $assocProperty->getReflection()->getAdapterResource();
                    /** @var \UniMapper\Entity\Reflection\Property\Option\Assoc $association */
                    $association = $assocProperty->getOption(Reflection\Property\Option\Assoc::KEY);
                    if (!isset($created[$alias])) {
                        $targetResource = $association->getTargetReflection()->getAdapterResource();
                        $targetPrimaryKey = $association->getTargetReflection()->getPrimaryProperty()->getUnmapped();
                        $sourcePrimaryKey = $association->getSourceReflection()->getPrimaryProperty()->getUnmapped();

                        switch ($association->getType()) {
                            case "m:n":
                            case "m>n":
                            case "m<n":
                                list($joinKey, $joinResource, $referencingKey) = $association->getBy();
                                $joinResourceAlias = $alias . '_' . $joinResource;
                                $result[] = "[{$joinResource}] AS [{$joinResourceAlias}] ON  [{$joinResourceAlias}].[{$joinKey}] = [{$table}].[{$sourcePrimaryKey}]";
                                $result[] = "[{$targetResource}] AS [{$alias}] ON  [{$alias}].[{$targetPrimaryKey}] = [{$joinResourceAlias}].[{$referencingKey}]";
                                break;
                            case "1:n":
                                list($referencedKey) = $association->getBy();
                                $result[] = "[{$targetResource}] AS [{$alias}] ON [{$alias}].[{$referencedKey}] = [{$table}].[{$sourcePrimaryKey}]";
                                break;
                            case "1:1":
                            case "n:1":
                                list($referencingKey) = $association->getBy();
                                $result[] = "[{$targetResource}] AS [{$alias}] ON [{$alias}].[{$targetPrimaryKey}] = [{$table}].[{$referencingKey}]";
                                break;
                        }
                        $created[$alias] = true;
                    }
                }
            }
        }

        return $result;
    }

    public function unmapFilterJoinProperty(Reflection $reflection, $name)
    {
        $assocDelimiterPos = strpos($name, '.');
        if ($assocDelimiterPos !== false) {
            $assocPropertyName = substr($name, 0, $assocDelimiterPos);
            $assocPropertyTargetName = substr($name, $assocDelimiterPos + 1);
            $assocProperty = $reflection->getProperty($assocPropertyName);
            $assocEntityReflection = \UniMapper\Entity\Reflection::load($assocProperty->getTypeOption());
            $property = $assocEntityReflection->getProperty($assocPropertyTargetName);
            return $assocProperty->getUnmapped() . '.' . $property->getUnmapped();
        }

        return $name;
    }

}