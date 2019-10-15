package conf

var Opts struct {
	Id                    string `short:"i" long:"id" default:"nimo-shake" description:"target database collection name"`
	LogLevel              string `short:"l" long:"logLevel" default:"info"`
	SourceAccessKeyID     string `short:"s" long:"sourceAccessKeyID" description:"dynamodb source access key id"`
	SourceSecretAccessKey string `long:"sourceSecretAccessKey" description:"dynamodb source secret access key"`
	SourceSessionToken    string `long:"sourceSessionToken" default:"" description:"dynamodb source session token"`
	SourceRegion          string `long:"sourceRegion" default:"" description:"dynamodb source region"`
	QpsFull               int    `long:"qpsFull" default:"" description:"qps of scan command, default is 10000"`
	QpsFullBatchNum       int64  `long:"qpsFullBatchNum" default:"" description:"batch number in each scan command, default is 128"`
	TargetAddress         string `short:"t" long:"targetAddress" description:"mongodb target address"`
	DiffOutputFile        string `short:"d" long:"diffOutputFile" default:"nimo-full-check-diff" description:"diff output file name"`
	Parallel              int    `short:"p" long:"parallel" default:"16" description:"how many threads used to compare, default is 16"`
	Sample                int    `short:"e" long:"sample" default:"1000" description:"comparison sample number for each table, 0 means disable"`
	//IndexPrimary          bool   `short:"m" long:"indexPrimary" description:"enable compare primary index"`
	//IndexUser             bool   `long:"indexUser" description:"enable compare user index"`
	FilterCollectionWhite string `long:"filterCollectionWhite" default:"" description:"only compare the given tables, split by ';'"`
	FilterCollectionBlack string `long:"filterCollectionBlack" default:"" description:"do not compare the given tables, split by ';'"`
	ConvertType           string `short:"c" long:"convertType" default:"raw" description:"convert type"`
	Version               bool   `short:"v" long:"version" description:"print version"`
}
