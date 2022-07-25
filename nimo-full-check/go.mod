module nimo-full-check

go 1.15

require (
	github.com/aws/aws-sdk-go v1.44.61
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/vinllen/log4go v0.0.0-20180514124125-3848a366df9d
	github.com/vinllen/mgo v0.0.0-20220329061231-e5ecea62f194
	nimo-shake v0.0.0-00010101000000-000000000000
)

replace nimo-shake => ../nimo-shake
