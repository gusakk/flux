package main

import (
	"os"

	"github.com/gusakk/flux/csv"
	"github.com/gusakk/flux/influxql"
	"github.com/gusakk/flux/memory"
	"github.com/spf13/cobra"
)

func v2(cmd *cobra.Command, args []string) error {
	for _, arg := range args {
		f, err := os.Open(arg)
		if err != nil {
			return err
		}

		dec := influxql.NewResultDecoder(&memory.Allocator{})
		results, err := dec.Decode(f)
		if err != nil {
			return err
		}

		enc := csv.NewMultiResultEncoder(csv.DefaultEncoderConfig())
		if _, err := enc.Encode(os.Stdout, results); err != nil {
			return err
		}
	}
	return nil
}
