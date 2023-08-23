package dag

import (
	"fmt"
	"go_shed/src/meta"
	"reflect"

	"github.com/rs/zerolog/log"
)

type Task interface {
	Id() string
	Execute()
}

// TaskExecuteSource returns Task's source code of its Execute() method. In case when method source code cannot be
// found in the AST (meta.PackagesASTsMap) string with message "NO IMPLEMENTATION FOUND..." would be returned. Though
// it should be the case only when whole new package is not added to the embedding (src/embed.go).
func TaskExecuteSource(t Task) string {
	tTypeName := reflect.TypeOf(t).Name()
	_, execMethodSource, err := meta.MethodBodySource(meta.PackagesASTsMap, tTypeName, "Execute")
	if err != nil {
		log.Error().Err(err).Msgf("Could not get %s.Execute() source code", tTypeName)
		return fmt.Sprintf("NO IMPLEMENTATION FOUND FOR %s.Execute()", tTypeName)
	}
	return execMethodSource
}
