package s3git

import (
	"errors"
)

// Get the full size unique hash for a given prefix.
// Return error in case none or multiple candidates are found
func (repo Repository) MakeUnique(prefix string) (string, error) {

	list, errList := repo.List(prefix)
	if errList != nil {
		return "", errList
	}

	err := errors.New("Not found (be less specific)")
	hash := ""
	for elem := range list {
		if len(hash) == 0 {
			hash, err = elem, nil
		} else {
			err = errors.New("More than one possiblity found (be more specific)")
		}
	}

	return hash, err
}
