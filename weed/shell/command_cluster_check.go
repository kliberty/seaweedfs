package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/cluster"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"io"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandClusterCheck{})
}

type commandClusterCheck struct {
}

func (c *commandClusterCheck) Name() string {
	return "cluster.check"
}

func (c *commandClusterCheck) Help() string {
	return `check current cluster network connectivity

	cluster.check

`
}

func (c *commandClusterCheck) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	clusterPsCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = clusterPsCommand.Parse(args); err != nil {
		return nil
	}

	// collect filers
	var filers []pb.ServerAddress
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
		})

		for _, node := range resp.ClusterNodes {
			filers = append(filers, pb.ServerAddress(node.Address))
		}
		return err
	})
	if err != nil {
		return
	}
	fmt.Fprintf(writer, "the cluster has %d filers: %+v\n", len(filers), filers)

	// collect volume servers
	var volumeServers []pb.ServerAddress
	t, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				volumeServers = append(volumeServers, pb.NewServerAddressFromDataNode(dn))
			}
		}
	}
	fmt.Fprintf(writer, "the cluster has %d volume servers: %+v\n", len(volumeServers), volumeServers)

	// collect all masters
	var masters []pb.ServerAddress
	for _, master := range commandEnv.MasterClient.GetMasters() {
		masters = append(masters, master)
	}

	// check from master to volume servers
	for _, master := range masters {
		for _, volumeServer := range volumeServers {
			fmt.Fprintf(writer, "checking master %s to volume server %s ... ", string(master), string(volumeServer))
			err := pb.WithMasterClient(false, master, commandEnv.option.GrpcDialOption, func(client master_pb.SeaweedClient) error {
				_, err := client.Ping(context.Background(), &master_pb.PingRequest{
					Target:     string(volumeServer),
					TargetType: cluster.VolumeServerType,
				})
				return err
			})
			if err == nil {
				fmt.Fprintf(writer, "ok\n")
			} else {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	// check from volume servers to masters
	for _, volumeServer := range volumeServers {
		for _, master := range masters {
			fmt.Fprintf(writer, "checking volume server %s to master %s ... ", string(volumeServer), string(master))
			err := pb.WithVolumeServerClient(false, volumeServer, commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
				_, err := client.Ping(context.Background(), &volume_server_pb.PingRequest{
					Target:     string(master),
					TargetType: cluster.MasterType,
				})
				return err
			})
			if err == nil {
				fmt.Fprintf(writer, "ok\n")
			} else {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	// check from filers to masters
	for _, filer := range filers {
		for _, master := range masters {
			fmt.Fprintf(writer, "checking filer %s to master %s ... ", string(filer), string(master))
			err := pb.WithFilerClient(false, filer, commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				_, err := client.Ping(context.Background(), &filer_pb.PingRequest{
					Target:     string(master),
					TargetType: cluster.MasterType,
				})
				return err
			})
			if err == nil {
				fmt.Fprintf(writer, "ok\n")
			} else {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	// check from filers to volume servers
	for _, filer := range filers {
		for _, volumeServer := range volumeServers {
			fmt.Fprintf(writer, "checking filer %s to volume server %s ... ", string(filer), string(volumeServer))
			err := pb.WithFilerClient(false, filer, commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				_, err := client.Ping(context.Background(), &filer_pb.PingRequest{
					Target:     string(volumeServer),
					TargetType: cluster.VolumeServerType,
				})
				return err
			})
			if err == nil {
				fmt.Fprintf(writer, "ok\n")
			} else {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	return nil
}
