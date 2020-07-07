package checkpoint

type Writer interface {
	// find current status
	FindStatus() (string, error)

	// update status
	UpdateStatus(status string) error

	// extract all checkpoint
	ExtractCheckpoint() (map[string]map[string]*Checkpoint, error)

	// extract single checkpoint
	ExtractSingleCheckpoint()

	// insert checkpoint
	Insert() error

	// update checkpoint
	Update() error

	// update with set
	UpdateWithSet() error

	// query
	Query() (Checkpoint, error)

	// drop
	Drop() error
}