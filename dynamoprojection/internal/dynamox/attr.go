package dynamox

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// attrAs fetches an attribute of type T from an item.
//
// It returns an error if the item is absent or a different type.
func attrAs[T types.AttributeValue](
	item map[string]types.AttributeValue,
	name string,
) (v T, err error) {
	v, ok, err := tryAttrAs[T](item, name)
	if err != nil {
		return v, err
	}
	if !ok {
		return v, fmt.Errorf("integrity error: missing %q attribute", name)
	}
	return v, nil
}

// tryAttrAs fetches an attribute of type T from an item.
//
// It returns an error if the item is a different type.
func tryAttrAs[T types.AttributeValue](
	item map[string]types.AttributeValue,
	name string,
) (v T, ok bool, err error) {
	a, ok := item[name]
	if !ok {
		return v, false, nil
	}

	v, ok = a.(T)
	if !ok {
		return v, false, fmt.Errorf(
			"integrity error: %q attribute should be %T not %T",
			name,
			v,
			a,
		)
	}

	return v, true, nil
}

// AsNumericString fetches the string representation of a numeric attribute from
// an item.
func AsNumericString(
	item map[string]types.AttributeValue,
	name string,
) (string, error) {
	attr, err := attrAs[*types.AttributeValueMemberN](item, name)
	if err != nil {
		return "", err
	}
	return attr.Value, nil
}

// AsBytes fetches a binary attribute from an item.
func AsBytes(
	item map[string]types.AttributeValue,
	name string,
) ([]byte, error) {
	v, err := attrAs[*types.AttributeValueMemberB](item, name)
	if err != nil {
		return nil, err
	}
	return v.Value, nil
}

// AsUint fetches an unsigned integer attribute from an item.
func AsUint[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](
	item map[string]types.AttributeValue,
	name string,
) (T, error) {
	attr, err := attrAs[*types.AttributeValueMemberN](item, name)
	if err != nil {
		return 0, err
	}

	v, err := strconv.ParseUint(attr.Value, 10, 64)
	if err != nil {
		return 0, err
	}

	return T(v), nil
}

// AsBool fetches a boolean attribute from an item. It returns false if the item
// is absent.
func AsBool(
	item map[string]types.AttributeValue,
	name string,
) (bool, error) {
	v, ok, err := tryAttrAs[*types.AttributeValueMemberBOOL](item, name)
	if !ok || err != nil {
		return false, err
	}
	return v.Value, nil
}

var (
	// True is a [types.AttributeValueMemberBOOL] for true.
	True = &types.AttributeValueMemberBOOL{Value: true}

	// False is a [types.AttributeValueMemberBOOL] for false.
	False = &types.AttributeValueMemberBOOL{Value: false}
)
