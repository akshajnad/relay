// Package envflag lets environment variables supply defaults for flags.
//
// Flags are declared with the standard flag package. Parse then fills every
// flag that was not given on the command line from PREFIX_FLAG_NAME, where
// the flag name is upper-cased and dashes become underscores
// (--redis-addr -> RELAY_REDIS_ADDR). Command-line values always win.
package envflag

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// EnvName returns the environment variable consulted for a flag.
func EnvName(prefix, flagName string) string {
	return prefix + "_" + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// Parse parses args into fs, then applies environment fallbacks for flags
// that were not set explicitly.
func Parse(fs *flag.FlagSet, prefix string, args []string) error {
	fs.VisitAll(func(f *flag.Flag) {
		f.Usage = fmt.Sprintf("%s (env %s)", f.Usage, EnvName(prefix, f.Name))
	})
	if err := fs.Parse(args); err != nil {
		return err
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	var err error
	fs.VisitAll(func(f *flag.Flag) {
		if err != nil || set[f.Name] {
			return
		}
		env := EnvName(prefix, f.Name)
		if v, ok := os.LookupEnv(env); ok {
			if e := fs.Set(f.Name, v); e != nil {
				err = fmt.Errorf("invalid value %q for %s: %w", v, env, e)
			}
		}
	})
	return err
}
