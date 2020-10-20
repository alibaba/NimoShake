package conf

type Configuration struct {
	Id                                string `config:"id"`
	LogFile                           string `config:"log.file"`
	LogLevel                          string `config:"log.level"`
	LogBuffer                         bool   `config:"log.buffer"`
	PidPath                           string `config:"pid_path"`
	SystemProfile                     int    `config:"system_profile"`
	HttpProfile                       int    `config:"http_profile"`
	SyncMode                          string `config:"sync_mode"`
	SourceAccessKeyID                 string `config:"source.access_key_id"`
	SourceSecretAccessKey             string `config:"source.secret_access_key"`
	SourceSessionToken                string `config:"source.session_token"`
	SourceRegion                      string `config:"source.region"`
	SourceSessionMaxRetries           uint   `config:"source.session.max_retries"`
	SourceSessionTimeout              uint   `config:"source.session.timeout"`
	QpsFull                           uint   `config:"qps.full"`
	QpsFullBatchNum                   int64  `config:"qps.full.batch_num"`
	QpsIncr                           uint   `config:"qps.incr"`
	QpsIncrBatchNum                   int64  `config:"qps.incr.batch_num"`
	FilterCollectionWhite             string `config:"filter.collection.white"`
	FilterCollectionBlack             string `config:"filter.collection.black"`
	TargetType                        string `config:"target.type"`
	TargetAddress                     string `config:"target.address"`
	TargetMongoDBType                 string `config:"target.mongodb.type"`
	TargetDBExist                     string `config:"target.db.exist"`
	SyncSchemaOnly                    bool   `config:"sync_schema_only"`
	FullConcurrency                   uint   `config:"full.concurrency"`
	FullDocumentConcurrency           uint   `config:"full.document.concurrency"`
	FullDocumentParser                uint   `config:"full.document.parser"`
	FullEnableIndexPrimary            bool   `config:"full.enable_index.primary"`
	FullEnableIndexUser               bool   `config:"full.enable_index.user"`
	FullExecutorInsertOnDupUpdate     bool   `config:"full.executor.insert_on_dup_update"`
	ConvertType                       string `config:"convert.type"`
	IncreaseConcurrency               uint   `config:"increase.concurrency"`
	IncreaseExecutorInsertOnDupUpdate bool   `config:"increase.executor.insert_on_dup_update"`
	IncreaseExecutorUpsert            bool   `config:"increase.executor.upsert"`
	CheckpointType                    string `config:"checkpoint.type"`
	CheckpointAddress                 string `config:"checkpoint.address"`
	CheckpointDb                      string `config:"checkpoint.db"`

	/*---------------------------------------------------------*/
	// generated variables
	Version string // version
}

var Options Configuration
