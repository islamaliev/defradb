// Copyright 2023 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"strconv"

	"github.com/spf13/cobra"

	"github.com/sourcenetwork/defradb/config"
	"github.com/sourcenetwork/defradb/http"
)

func MakeTxDiscardCommand(cfg *config.Config) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "discard [id]",
		Short: "Discard a DefraDB transaction.",
		Long:  `Discard a DefraDB transaction.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return err
			}
			tx, err := http.NewTransaction(cfg.API.Address, id)
			if err != nil {
				return err
			}
			tx.Discard(cmd.Context())
			return nil
		},
	}
	return cmd
}
