package cli

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

type remoteHost struct {
	HostID             string `json:"hostId"`
	Address            string `json:"address"`
	Label              string `json:"label,omitempty"`
	State              string `json:"state"`
	LastProbeAt        string `json:"lastProbeAt"`
	LastProbeSucceeded bool   `json:"lastProbeSucceeded"`
	LastProbeError     string `json:"lastProbeError,omitempty"`
}

type remoteHostResult struct {
	RemoteHost remoteHost `json:"remoteHost"`
}

type remoteHostListResult struct {
	RemoteHosts []remoteHost `json:"remoteHosts"`
}

type remoteHostRemoveResult struct {
	HostID string `json:"hostId"`
}

type addRemoteHostRequest struct {
	HostID  string `json:"hostId"`
	Address string `json:"address"`
	Label   string `json:"label,omitempty"`
}

type updateRemoteHostStateRequest struct {
	State string `json:"state"`
}

func newRemoteHostCommand(ctx *commandContext) *cobra.Command {
	cmd := &cobra.Command{Use: "remote-host", Aliases: []string{"remote-hosts"}, Short: "Manage remote daemon hosts"}
	cmd.AddCommand(newRemoteHostAddCommand(ctx))
	cmd.AddCommand(newRemoteHostListCommand(ctx))
	cmd.AddCommand(newRemoteHostStatusCommand(ctx))
	cmd.AddCommand(newRemoteHostStateCommand(ctx))
	cmd.AddCommand(newRemoteHostRemoveCommand(ctx))
	return cmd
}

func newRemoteHostAddCommand(ctx *commandContext) *cobra.Command {
	var id, address, label string
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Register a remote daemon host",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(id) == "" || strings.TrimSpace(address) == "" {
				return usageError{errors.New("usage: --id and --address are required")}
			}
			var res remoteHostResult
			if err := ctx.postJSON(cmd.Context(), "remote-hosts", addRemoteHostRequest{HostID: id, Address: address, Label: label}, &res); err != nil {
				return err
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "registered remote host %s (%s)\n", res.RemoteHost.HostID, res.RemoteHost.State)
			return err
		},
	}
	cmd.Flags().StringVar(&id, "id", "", "Stable lowercase host id (required)")
	cmd.Flags().StringVar(&address, "address", "", "Reachable remote daemon host:port (required)")
	cmd.Flags().StringVar(&label, "label", "", "Optional human label")
	return cmd
}

func newRemoteHostListCommand(ctx *commandContext) *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List registered remote hosts",
		Args:    noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var res remoteHostListResult
			if err := ctx.getJSON(cmd.Context(), "remote-hosts", &res); err != nil {
				return err
			}
			sort.Slice(res.RemoteHosts, func(i, j int) bool { return res.RemoteHosts[i].HostID < res.RemoteHosts[j].HostID })
			if jsonOutput {
				return writeJSON(cmd.OutOrStdout(), res)
			}
			return writeRemoteHostList(cmd, res.RemoteHosts)
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output remote hosts as JSON")
	return cmd
}

func newRemoteHostStatusCommand(ctx *commandContext) *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "status <host-id>",
		Short: "Show one remote host's connection health",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			var res remoteHostResult
			if err := ctx.getJSON(cmd.Context(), "remote-hosts/"+url.PathEscape(strings.TrimSpace(args[0])), &res); err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(cmd.OutOrStdout(), res)
			}
			return writeRemoteHostStatus(cmd, res.RemoteHost)
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output remote host as JSON")
	return cmd
}

func newRemoteHostStateCommand(ctx *commandContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "state <host-id> <available|stopped|destroyed>",
		Short: "Declare remote host state or resume health probing",
		Args:  usageArgs(cobra.ExactArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			var res remoteHostResult
			if err := ctx.patchJSON(cmd.Context(), "remote-hosts/"+url.PathEscape(strings.TrimSpace(args[0]))+"/state", updateRemoteHostStateRequest{State: strings.TrimSpace(args[1])}, &res); err != nil {
				return err
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "remote host %s is %s\n", res.RemoteHost.HostID, res.RemoteHost.State)
			return err
		},
	}
	return cmd
}

func newRemoteHostRemoveCommand(ctx *commandContext) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:     "rm <host-id>",
		Aliases: []string{"remove", "delete"},
		Short:   "Deregister a remote host",
		Args:    usageArgs(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := strings.TrimSpace(args[0])
			if !yes {
				confirmed, err := confirm(cmd.InOrStdin(), cmd.OutOrStdout(), "Deregister remote host "+id+"? [y/N] ", false)
				if err != nil {
					return err
				}
				if !confirmed {
					_, err := fmt.Fprintln(cmd.OutOrStdout(), "aborted")
					return err
				}
			}
			var res remoteHostRemoveResult
			if err := ctx.deleteJSON(cmd.Context(), "remote-hosts/"+url.PathEscape(id), &res); err != nil {
				return err
			}
			if res.HostID == "" {
				res.HostID = id
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "deregistered remote host %s\n", res.HostID)
			return err
		},
	}
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "Skip confirmation prompt")
	return cmd
}

func writeRemoteHostList(cmd *cobra.Command, hosts []remoteHost) error {
	if len(hosts) == 0 {
		_, err := fmt.Fprintln(cmd.OutOrStdout(), "No remote hosts registered.")
		return err
	}
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tLABEL\tADDRESS\tSTATE\tREASON"); err != nil {
		return err
	}
	for _, host := range hosts {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", host.HostID, host.Label, host.Address, host.State, host.LastProbeError); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeRemoteHostStatus(cmd *cobra.Command, host remoteHost) error {
	_, err := fmt.Fprintf(cmd.OutOrStdout(), "ID: %s\nLabel: %s\nAddress: %s\nState: %s\nLast probe: %s\nLast probe succeeded: %t\nReason: %s\n", host.HostID, host.Label, host.Address, host.State, host.LastProbeAt, host.LastProbeSucceeded, host.LastProbeError)
	return err
}
