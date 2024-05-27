package metcd

import (
	"reflect"
	"strings"

	"github.com/k3s-io/kine/pkg/client"
)

var (
	partitionTree        map[string]map[string]*revisionClient = map[string]map[string]*revisionClient{}
	lastRevision         int64
	currentRevisions     map[int64]map[string]map[string]int64 = map[int64]map[string]map[string]int64{}
	currentRevisionsLock chan bool                             = make(chan bool, 1)
)

type revisionClient struct {
	client.Client
	revision int64
	branch   string
	leaf     string
}

func (rc *revisionClient) updateRevision(revision int64) {
	if revision == 0 {
		return
	}

	currentRevisionsLock <- true
	for rc.revision < revision {
		rc.revision++
		createRevisionSnapshot()
	}
	<-currentRevisionsLock
}

// Don't use this function without lock
var createRevisionSnapshot = func() int64 {
	currRevs := map[string]map[string]int64{}
	for b := range partitionTree {
		currRevs[b] = map[string]int64{}

		for l := range partitionTree[b] {
			currRevs[b][l] = partitionTree[b][l].revision
		}
	}

	if !reflect.DeepEqual(currentRevisions[lastRevision], currRevs) {
		lastRevision++

		currentRevisions[lastRevision] = currRevs // TODO GC
	}

	return lastRevision
}

func currentRevision() int64 {
	currentRevisionsLock <- true
	defer func() {
		<-currentRevisionsLock
	}()

	return createRevisionSnapshot()
}

func revToOrig(revision int64, client *revisionClient) int64 {
	if revision == 0 {
		return revision
	}

	currentRevisionsLock <- true
	revisions, ok := currentRevisions[revision]
	<-currentRevisionsLock
	if !ok {
		panic("missing revision, this error should not happen")
	}

	revision, ok = revisions[client.branch][client.leaf]
	if !ok {
		panic("missing revToOrig, this error should not happen")
	}

	return revision
}

func origToRev(client *revisionClient, revision int64) int64 {
	if revision == 0 {
		return revision
	}

	currentRevisionsLock <- true
	defer func() {
		<-currentRevisionsLock
	}()

	var highest int64
	for keyRev, currRevs := range currentRevisions {
		if currRevs[client.branch][client.leaf] == revision {
			if keyRev > highest {
				highest = keyRev
			}
		}
	}

	if highest == 0 {
		panic("missing origToRev, this error should not happen")
	}

	return highest
}

func getClient(key string) *revisionClient {
	c := partitionTree[""][""]

	for part := range partitionTree {
		if part == "" {
			continue
		}

		if strings.HasPrefix(key, part) {
			ns := strings.Split(strings.TrimPrefix(key, part), "/")[0]

			var ok bool
			c, ok = partitionTree[part][ns]
			if !ok {
				c = partitionTree[part][""]
			}

			break
		}
	}

	return c
}

func getClients(prefix string) []*revisionClient {
	clients := []*revisionClient{}

	for part := range partitionTree {
		if prefix != part {
			continue
		}

		for i := range partitionTree[part] {
			clients = append(clients, partitionTree[part][i])
		}
	}

	if len(clients) == 0 {
		clients = append(clients, partitionTree[""][""])
	}

	return clients
}
