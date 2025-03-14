module nimo-full-check

go 1.15

require (
	github.com/aws/aws-sdk-go v1.44.61
	github.com/google/go-cmp v0.6.0
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/vinllen/log4go v0.0.0-20180514124125-3848a366df9d
	go.mongodb.org/mongo-driver v1.16.1
	nimo-shake v0.0.0-00010101000000-000000000000
)

replace nimo-shake => ../nimo-shake
