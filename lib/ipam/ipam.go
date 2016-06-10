package ipam

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"log"
	"math"
	"net"
	"os"
	"strings"
	"time"
)

const (
	RETRIES           = 100
	KEY_ERROR_RETRIES = 3

	// IPAM paths
	IPAM_V_PATH             = "/calico/ipam/v2/"
	IPAM_CONFIG_PATH        = IPAM_V_PATH + "config"
	IPAM_HOSTS_PATH         = IPAM_V_PATH + "host"
	IPAM_HOST_PATH          = IPAM_HOSTS_PATH + "/%s"
	IPAM_HOST_AFFINITY_PATH = IPAM_HOST_PATH + "/ipv%d/block/"
	IPAM_BLOCK_PATH         = IPAM_V_PATH + "assignment/ipv%d/block/"
	IPAM_HANDLE_PATH        = IPAM_V_PATH + "handle/"
)

type IPAMConfig struct {
	StrictAffinity     bool
	AutoAllocateBlocks bool
}

type AssignmentArgs struct {
	Num4     int64
	Num6     int64
	HandleId string
	Attrs    map[string]string
	Hostname *string
	IPv4Pool *net.IPNet
	IPv6Pool *net.IPNet
}

type BlockReaderWriter struct {
	etcd client.KeysAPI
}

func (c IPAMClient) AutoAssign(args AssignmentArgs) ([]net.IP, []net.IP, error) {
	// Determine the hostname to use - prefer the provided hostname if
	// non-nil, otherwise use the hostname reported by os.
	log.Printf("Auto-assign args: %+v", args)
	log.Printf("Auto-assign %d ipv4, %d ipv6 addrs", args.Num4, args.Num6)
	hostname := decideHostname(args.Hostname)
	log.Printf("Assigning for host: %s", hostname)

	// Assign addresses.
	var err error
	var v4list, v6list []net.IP
	v4list, err = c.autoAssign(args.Num4, args.HandleId, args.Attrs, args.IPv4Pool, IPv4, hostname)
	if err != nil {
		log.Printf("Error assigning IPV4 addresses: %s", err)
	} else {
		// If no err assigning V4, try to assign any V6.
		v6list, err = c.autoAssign(args.Num6, args.HandleId, args.Attrs, args.IPv6Pool, IPv6, hostname)
	}

	return v4list, v6list, err
}

func (c IPAMClient) autoAssign(num int64, handleId string, attrs map[string]string, pool *net.IPNet, version IPVersion, host string) ([]net.IP, error) {

	// Start by trying to assign from one of the host-affine blocks.  We
	// always do strict checking at this stage, so it doesn't matter whether
	// globally we have strict_affinity or not.
	log.Printf("Looking for addresses in current affine blocks for host %s", host)
	affBlocks, err := c.BlockReaderWriter.getAffineBlocks(host, version, pool)
	if err != nil {
		return nil, err
	}
	log.Printf("Found %d affine IPv%d blocks", len(affBlocks), version.Number)
	ips := []net.IP{}
	for int64(len(ips)) < num {
		if len(affBlocks) == 0 {
			log.Println("Ran out of affine blocks for host", host)
			break
		}
		cidr := affBlocks[0]
		affBlocks = affBlocks[1:]
		ips, _ = c.assignFromExistingBlock(cidr, num, handleId, attrs, host, nil)
		log.Println("Block provided addresses:", ips)
	}

	// If there are still addresses to allocate, then we've run out of
	// blocks with affinity.  Before we can assign new blocks or assign in
	// non-affine blocks, we need to check that our IPAM configuration
	// allows that.
	// TODO: Read config from etcd
	config := IPAMConfig{StrictAffinity: false, AutoAllocateBlocks: true}
	if config.AutoAllocateBlocks == true {
		rem := num - int64(len(ips))
		log.Printf("Need to allocate %d more addresses", rem)
		// TODO: Limit number of retries.
		retries := RETRIES
		for rem > 0 {
			// Claim a new block.
			b, err := c.BlockReaderWriter.ClaimNewAffineBlock(host, version, pool, config)
			if err != nil {
				log.Println("Error claiming new block:", err)
				retries = retries - 1
				if retries == 0 {
					log.Println("Max retries hit")
					return nil, errors.New("Max retries hit")
				}
			} else {
				// Claim successful.  Assign addresses from the new block.
				log.Println("Claimed new block - assigning addresses")
				newIPs, err := c.assignFromExistingBlock(*b, rem, handleId, attrs, host, &config.StrictAffinity)
				if err != nil {
					log.Println("Error assigning IPs:", err)
					break
				}
				ips = append(ips, newIPs...)
				rem = num - int64(len(ips))
			}
		}
	}

	// If there are still addresses to allocate, we've now tried all blocks
	// with some affinity to us, and tried (and failed) to allocate new
	// ones.  If we do not require strict host affinity, our last option is
	// a random hunt through any blocks we haven't yet tried.
	//
	// Note that this processing simply takes all of the IP pools and breaks
	// them up into block-sized CIDRs, then shuffles and searches through each
	// CIDR.  This algorithm does not work if we disallow auto-allocation of
	// blocks because the allocated blocks may be sparsely populated in the
	// pools resulting in a very slow search for free addresses.
	//
	// If we need to support non-strict affinity and no auto-allocation of
	// blocks, then we should query the actual allocation blocks and assign
	// from those.
	if config.StrictAffinity != true {
		log.Println("Attempting to assign from non-affine block")
		// TODO: this
	}

	return ips, nil
}

func (c IPAMClient) AssignIP(addr net.IP, handleId string, attrs map[string]string, host *string) error {
	hostname := decideHostname(host)
	log.Printf("Assigning IP %s to host: %s", addr, hostname)

	blockCidr := GetBlockCIDRForAddress(addr)
	// TODO: Wrap this up in retry loop.
	for i := 0; i < RETRIES; i++ {
		block, err := c.BlockReaderWriter.ReadBlock(blockCidr)
		if err != nil {
			// Block doesn't exist, we need to create it.
			// TODO: check returned error type to ensure this
			// is correct.
			// TODO: Validate the given IP address is in a configured pool.
			log.Printf("Block for IP %s does not yet exist, creating", addr)
			cfg := IPAMConfig{StrictAffinity: false, AutoAllocateBlocks: true}
			version := GetIPVersion(addr)
			newBlockCidr, err := c.BlockReaderWriter.ClaimNewAffineBlock(hostname, version, nil, cfg)
			if err != nil {
				// TODO: Check type of error.
				log.Printf("Someone else claimed block before us")
				continue
			}
			log.Printf("Claimed new block: %s", newBlockCidr)
			continue
		}
		log.Printf("IP %s is in block %s", addr, block.Cidr)
		err = block.Assign(addr, handleId, attrs, hostname)
		if err != nil {
			log.Printf("Failed to assign address %s: %s", addr, err)
			return err
		}

		// Increment handle.
		c.incrementHandle(handleId, blockCidr, int64(1))

		// Update the block.
		err = c.BlockReaderWriter.CompareAndSwapBlock(*block)
		if err != nil {
			c.decrementHandle(handleId, blockCidr, int64(1))
			log.Println("CAS failed on block %s", block.Cidr)
			return err
		}
		return nil
	}
	return errors.New("Max retries hit")
}

func (c IPAMClient) ReleaseIPs(ips []net.IP) ([]net.IP, error) {
	log.Println("Releasing IP addresses:", ips)
	unallocated := []net.IP{}
	for _, ip := range ips {
		blockCidr := GetBlockCIDRForAddress(ip)
		// TODO: Group IP addresses per-block to minimize writes to etcd.
		unalloc, err := c.releaseIPsFromBlock([]net.IP{ip}, blockCidr)
		if err != nil {
			log.Println("Error releasing IPs:", err)
			return nil, err
		}
		unallocated = append(unallocated, unalloc...)
	}
	return unallocated, nil
}

func (c IPAMClient) releaseIPsFromBlock(ips []net.IP, blockCidr net.IPNet) ([]net.IP, error) {
	for i := 0; i < RETRIES; i++ {
		b, err := c.BlockReaderWriter.ReadBlock(blockCidr)
		if err != nil {
			// TODO: Check type of error.
			// The block does not exist - all addresses must be unassigned.
			return ips, nil
		}

		// Block exists - release the IPs from it.
		unallocated, handles, err2 := b.Release(ips)
		if err2 != nil {
			return nil, err2
		}
		if len(ips) == len(unallocated) {
			// All the given IP addresses are already unallocated.
			// Just return.
			return unallocated, nil
		}

		// If the block is empty and has no affinity, we can delete it.
		// Otherwise, update the block using CAS.
		var casError error
		if b.Empty() && b.HostAffinity == "" {
			log.Println("Deleting non-affine block")
			casError = c.BlockReaderWriter.DeleteBlock(*b)
		} else {
			log.Println("Updating assignments in block")
			casError = c.BlockReaderWriter.CompareAndSwapBlock(*b)
		}

		if casError != nil {
			log.Printf("Error updating block - retry #%d", i)
			continue
		}

		// Success - decrement handles.
		log.Println("Decrementing handles:", handles)
		for handleId, amount := range handles {
			c.decrementHandle(handleId, blockCidr, amount)
		}
		return unallocated, nil
	}
	return nil, errors.New("Max retries hit")
}

func (c IPAMClient) assignFromExistingBlock(
	blockCidr net.IPNet, num int64, handleId string, attrs map[string]string, host string, affCheck *bool) ([]net.IP, error) {
	// Limit number of retries.
	var ips []net.IP
	for i := 0; i < RETRIES; i++ {
		log.Printf("Auto-assign from %s - retry %d", blockCidr, i)
		b, err := c.BlockReaderWriter.ReadBlock(blockCidr)
		if err != nil {
			return nil, err
		}
		log.Println("Got block:", b)
		ips, err = b.AutoAssign(num, handleId, host, attrs, true)
		if err != nil {
			log.Println("Error in auto assign:", err)
			return nil, err
		}
		if len(ips) == 0 {
			log.Printf("Block %s is full", blockCidr)
			return []net.IP{}, nil
		}

		// Increment handle count.
		c.incrementHandle(handleId, blockCidr, num)

		// Update the block using CAS.
		err = c.BlockReaderWriter.CompareAndSwapBlock(*b)
		if err != nil {
			c.decrementHandle(handleId, blockCidr, num)
			log.Println("Error updating block - try again")
			continue
		}
		break
	}
	return ips, nil
}

func (c IPAMClient) IPsByHandle(handleId string) ([]net.IP, error) {
	handle, err := c.readHandle(handleId)
	if err != nil {
		return nil, err
	}

	assignments := []net.IP{}
	for k, _ := range handle.Block {
		_, blockCidr, _ := net.ParseCIDR(k)
		b, err := c.BlockReaderWriter.ReadBlock(*blockCidr)
		if err != nil {
			log.Printf("Couldn't read block %s referenced by handle %s", blockCidr, handleId)
			continue
		}
		assignments = append(assignments, b.IPsByHandle(handleId)...)
	}
	return assignments, nil
}

func (c IPAMClient) ReleaseByHandle(handleId string) error {
	handle, err := c.readHandle(handleId)
	if err != nil {
		return err
	}

	for blockStr, _ := range handle.Block {
		_, blockCidr, _ := net.ParseCIDR(blockStr)
		err = c.releaseByHandle(handleId, *blockCidr)
	}
	return nil
}

func (c IPAMClient) releaseByHandle(handleId string, blockCidr net.IPNet) error {
	for i := 0; i < RETRIES; i++ {
		block, err := c.BlockReaderWriter.ReadBlock(blockCidr)
		if err != nil {
			// TODO: Check type of error.
			// Block doesn't exist, so all addresses are already
			// unallocated.  This can happen when a handle is
			// overestimating the number of assigned addresses.
			return nil
		}
		num := block.ReleaseByHandle(handleId)
		if num == 0 {
			// Block has no addresses with this handle, so
			// all addresses are already unallocated.
			return nil
		}

		err = c.BlockReaderWriter.CompareAndSwapBlock(*block)
		if err != nil {
			// Failed to update - retry.
			continue
		}

		// TODO: Deal with the "None" handle...
		c.decrementHandle(handleId, blockCidr, num)
	}
	return errors.New("Hit max retries")
}

func (c IPAMClient) readHandle(handleId string) (*AllocationHandle, error) {
	key := IPAM_HANDLE_PATH + handleId
	opts := client.GetOptions{Quorum: true}
	resp, err := c.BlockReaderWriter.etcd.Get(context.Background(), key, &opts)
	if err != nil {
		log.Println("Error reading IPAM handle:", err)
		return nil, err
	}
	h := AllocationHandle{}
	json.Unmarshal([]byte(resp.Node.Value), &h)
	h.DbResult = resp.Node.Value
	return &h, nil
}

func (c IPAMClient) incrementHandle(handleId string, blockCidr net.IPNet, num int64) error {
	for i := 0; i < RETRIES; i++ {
		handle, err := c.readHandle(handleId)
		if err != nil {
			// TODO: Check error type.
			// Handle doesn't exist - create it.
			log.Println("Creating new handle:", handleId)
			handle = &AllocationHandle{
				HandleId: handleId,
				Block:    map[string]int64{},
			}
		}

		// Increment the handle for this block.
		handle.IncrementBlock(blockCidr, num)
		err = c.compareAndSwapHandle(*handle)
		if err != nil {
			continue
		}
		return nil
	}
	return errors.New("Max retries hit")

}

func (c IPAMClient) decrementHandle(handleId string, blockCidr net.IPNet, num int64) error {
	for i := 0; i < RETRIES; i++ {
		handle, err := c.readHandle(handleId)
		if err != nil {
			log.Fatal("Can't decrement block because it doesn't exist")
		}

		_, err = handle.DecrementBlock(blockCidr, num)
		if err != nil {
			// TODO: Check error type.
			log.Fatal("Can't decrement block - too few allocated")
		}

		err = c.compareAndSwapHandle(*handle)
		if err != nil {
			continue
		}
		log.Printf("Decremented handle '%s' by %d", handleId, num)
		return nil
	}
	return errors.New("Max retries hit")
}

func (c IPAMClient) compareAndSwapHandle(h AllocationHandle) error {
	// If the block has a store result, compare and swap agianst that.
	var opts client.SetOptions
	key := IPAM_HANDLE_PATH + h.HandleId

	// Determine correct Set options.
	if h.DbResult != "" {
		if h.Empty() {
			// The handle is empty - delete it instead of an update.
			log.Println("CAS delete handle:", h.HandleId)
			deleteOpts := client.DeleteOptions{PrevValue: h.DbResult}
			_, err := c.BlockReaderWriter.etcd.Delete(context.Background(),
				key, &deleteOpts)
			return err
		}
		log.Println("CAS update handle:", h.HandleId)
		opts = client.SetOptions{PrevExist: client.PrevExist, PrevValue: h.DbResult}
	} else {
		log.Println("CAS write new handle:", h.HandleId)
		opts = client.SetOptions{PrevExist: client.PrevNoExist}
	}

	j, err := json.Marshal(h)
	if err != nil {
		log.Println("Error converting handle to json:", err)
		return err
	}
	_, err = c.BlockReaderWriter.etcd.Set(context.Background(), key, string(j), &opts)
	if err != nil {
		log.Println("CAS error writing json:", err)
		return err
	}

	return nil
}

func (rw BlockReaderWriter) getAffineBlocks(host string, ver IPVersion, pool *net.IPNet) ([]net.IPNet, error) {
	key := fmt.Sprintf(IPAM_HOST_AFFINITY_PATH, host, ver.Number)
	opts := client.GetOptions{Quorum: true, Recursive: true}
	res, err := rw.etcd.Get(context.Background(), key, &opts)
	if err != nil {
		log.Println("Error reading blocks from etcd", err)
		return nil, err
	}
	log.Println("Read blocks from etcd:", res)

	ids := []net.IPNet{}
	if res.Node != nil {
		for _, n := range res.Node.Nodes {
			if !n.Dir {
				// Extract the block identifier (subnet) which is encoded
				// into the etcd key.  We need to replace "-" with "/" to
				// turn it back into a cidr.
				log.Printf("Found block on host %s: %s", host, n.Key)
				ss := strings.Split(n.Key, "/")
				_, id, _ := net.ParseCIDR(strings.Replace(ss[len(ss)-1], "-", "/", 1))
				ids = append(ids, *id)
			}
		}
	}
	return ids, nil
}

func (rw BlockReaderWriter) ClaimNewAffineBlock(
	host string, version IPVersion, pool *net.IPNet, config IPAMConfig) (*net.IPNet, error) {

	// If pool is not nil, use the given pool.  Otherwise, default to
	// all configured pools.
	var pools []net.IPNet
	if pool != nil {
		// TODO: Validate the given pool is actually configured.
		pools = []net.IPNet{*pool}
	} else {
		// TODO: Default to all configured pools.
		_, p, _ := net.ParseCIDR("192.168.0.0/16")
		pools = []net.IPNet{*p}
	}

	// Iterate through pools to find a new block.
	log.Println("Claiming a new affine block for host", host)
	for _, pool := range pools {
		for _, subnet := range Blocks(pool) {
			// Check if a block already exists for this subnet.
			key := blockDatastorePath(subnet)
			_, err := rw.etcd.Get(context.Background(), key, nil)
			if client.IsKeyNotFound(err) {
				// The block does not yet exist in etcd.  Try to grab it.
				log.Println("Found free block:", subnet)
				rw.claimBlockAffinity(subnet, host, config)
				return &subnet, nil
			} else if err != nil {
				log.Println("Error checking block:", err)
				return nil, err
			}
		}
	}
	return nil, errors.New("No free blocks")
}

func (rw BlockReaderWriter) claimBlockAffinity(subnet net.IPNet, host string, config IPAMConfig) error {
	// Claim the block in etcd.
	log.Printf("Host %s claiming block affinity for %s", host, subnet)
	affinityPath := blockHostAffinityPath(subnet, host)
	rw.etcd.Set(context.Background(), affinityPath, "", nil)

	// Create the new block.
	block := NewBlock(subnet)
	block.HostAffinity = host
	block.StrictAffinity = config.StrictAffinity

	// Compare and swap the new block.
	err := rw.CompareAndSwapBlock(block)
	if err != nil {
		// Block already exists, check affinity.
		// TODO: Check type of returned error.
		log.Println("Error claiming block affinity:", err)
		b, err := rw.ReadBlock(subnet)
		if err != nil {
			log.Println("Error reading block:", err)
			return err
		}
		if b.HostAffinity == host {
			// Block has affinity to this host, meaning another
			// process on this host claimed it.
			log.Printf("Block %s already claimed by us.  Success", subnet)
			return nil
		}

		// Some other host beat us to this block.  Cleanup and return error.
		rw.etcd.Delete(context.Background(), affinityPath, &client.DeleteOptions{})
		return errors.New(fmt.Sprintf("Block %s already claimed by %s", subnet, b.HostAffinity))
	}
	return nil
}

func (rw BlockReaderWriter) CompareAndSwapBlock(b AllocationBlock) error {
	// If the block has a store result, compare and swap agianst that.
	var opts client.SetOptions
	key := blockDatastorePath(b.Cidr)

	// Determine correct Set options.
	if b.DbResult != "" {
		log.Println("CAS update block:", b.Cidr)
		opts = client.SetOptions{PrevExist: client.PrevExist, PrevValue: b.DbResult}
	} else {
		log.Println("CAS write new block:", b.Cidr)
		opts = client.SetOptions{PrevExist: client.PrevNoExist}
	}

	j, err := json.Marshal(b)
	if err != nil {
		log.Println("Error converting block to json:", err)
		return err
	}
	_, err = rw.etcd.Set(context.Background(), key, string(j), &opts)
	if err != nil {
		log.Println("CAS error writing block:", err)
		return err
	}

	return nil
}

func (rw BlockReaderWriter) DeleteBlock(b AllocationBlock) error {
	opts := client.DeleteOptions{PrevValue: b.DbResult}
	key := blockDatastorePath(b.Cidr)
	_, err := rw.etcd.Delete(context.Background(), key, &opts)
	return err
}

func (rw BlockReaderWriter) ReadBlock(blockCidr net.IPNet) (*AllocationBlock, error) {
	key := blockDatastorePath(blockCidr)
	opts := client.GetOptions{Quorum: true}
	resp, err := rw.etcd.Get(context.Background(), key, &opts)
	if err != nil {
		log.Println("Error reading IPAM block:", err)
		return nil, err
	}
	log.Println("Response from etcd:", resp)
	b := NewBlock(blockCidr)
	json.Unmarshal([]byte(resp.Node.Value), &b)
	b.DbResult = resp.Node.Value
	return &b, nil
}

// Return the list of block CIDRs which fall within
// the given pool.
func Blocks(pool net.IPNet) []net.IPNet {
	// Determine the IP type to use.
	ipVersion := GetIPVersion(pool.IP)
	nets := []net.IPNet{}
	ip := pool.IP
	size := int64(math.Exp2(float64(ipVersion.TotalBits - ipVersion.BlockPrefixLength)))
	for pool.Contains(ip) {
		nets = append(nets, net.IPNet{ip, ipVersion.BlockPrefixMask})
		ip = IncrementIP(ip, size)
	}
	log.Printf("%s has %d subnets of size %d", pool, len(nets), size)
	return nets
}

func blockDatastorePath(blockCidr net.IPNet) string {
	version := GetIPVersion(blockCidr.IP)
	path := fmt.Sprintf(IPAM_BLOCK_PATH, version.Number)
	return path + strings.Replace(blockCidr.String(), "/", "-", 1)
}

func blockHostAffinityPath(blockCidr net.IPNet, host string) string {
	version := GetIPVersion(blockCidr.IP)
	path := fmt.Sprintf(IPAM_HOST_AFFINITY_PATH, host, version.Number)
	return path + strings.Replace(blockCidr.String(), "/", "-", 1)
}

type IPAMClient struct {
	BlockReaderWriter BlockReaderWriter
}

func decideHostname(host *string) string {
	// Determine the hostname to use - prefer the provided hostname if
	// non-nil, otherwise use the hostname reported by os.
	var hostname string
	var err error
	if host != nil {
		hostname = *host
	} else {
		hostname, err = os.Hostname()
		if err != nil {
			log.Fatal("Failed to acquire hostname")
		}
	}
	return hostname
}

func NewIPAMClient() (*IPAMClient, error) {
	// Create the interface into etcd for blocks.
	log.Println("Creating new IPAM client")
	config := client.Config{
		Endpoints:               []string{"http://localhost:2379"},
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(config)
	if err != nil {
		log.Println("Failed to configure etcd client")
		return nil, err
	}
	api := client.NewKeysAPI(c)
	b := BlockReaderWriter{etcd: api}

	return &IPAMClient{BlockReaderWriter: b}, nil
}
