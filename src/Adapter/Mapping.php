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

    /**
     * @param \UniMapper\Entity\Reflection $reflection
     * @param array                        $filter
     *
     * @return array
     */
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
                    $alias = $assocProperty->getName(true);
                    $table = $assocProperty->getEntityReflection()->getAdapterResource();
                    $association = $assocProperty->getAssociation();
                    if (!isset($created[$alias])) {
                        if ($association instanceof Association\ManyToMany) {
                            // M:N
                            $joinResourceAlias = $alias . '_' . $association->getJoinResource();
                            $result[] = "[{$association->getJoinResource()}] AS [{$joinResourceAlias}] ON  [{$joinResourceAlias}].[{$association->getJoinReferencingKey()}] = [{$table}].[{$association->getReferencingKey()}]";
                            $result[] = "[{$association->getTargetResource()}] AS [{$alias}] ON  [{$alias}].[{$association->getReferencedKey()}] = [{$joinResourceAlias}].[{$association->getJoinReferencedKey()}]";
                        } else {
                            // N:1, 1:1, 1:N
                            $result[] = "[{$association->getTargetResource()}] AS [{$alias}] ON [{$alias}].[{$association->getReferencedKey()}] = [{$table}].[{$association->getReferencingKey()}]";
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
            return $assocProperty->getName(true) . '.' . $property->getName(true);
        }

        return $name;
    }

}