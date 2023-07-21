package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

const (
	accessKeyID   = "YOUR_AWS_ACCESS_KEY_ID"
	secureKey     = "YOUR_AWS_SECRET_ACCESS_KEY"
	clusterRegion = "YOUR_AWS_REGION"
)

// AwsClient struct
type AwsClient struct {
	svc *ec2.EC2
}

// InitAwsSession init session
func InitAwsSession() *AwsClient {
	os.Setenv("AWS_ACCESS_KEY_ID", accessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", secureKey)
	os.Setenv("AWS_REGION", clusterRegion)

	mySession := session.Must(session.NewSession())
	aClient := &AwsClient{
		svc: ec2.New(mySession),
	}

	return aClient
}

func (a *AwsClient) GetAWSMetadata(hostname string) (map[string]interface{}, error) {
	filters := []*ec2.Filter{
		{
			Name: aws.String("private-dns-name"),
			Values: []*string{
				aws.String(hostname),
			},
		},
	}
	input := ec2.DescribeInstancesInput{Filters: filters}
	instanceInfo, err := a.svc.DescribeInstances(&input)

	if err != nil {
		return nil, err
	}

	if len(instanceInfo.Reservations) < 1 {
		return nil, fmt.Errorf("No instance found in current cluster with name %s", hostname)
	}

	instance := instanceInfo.Reservations[0].Instances[0]
	data := map[string]interface{}{
		"InstanceID":        aws.StringValue(instance.InstanceId),
		"InstanceType":      aws.StringValue(instance.InstanceType),
		"PrivateIP":         aws.StringValue(instance.PrivateIpAddress),
		"PublicIP":          aws.StringValue(instance.PublicIpAddress),
		"AvailabilityZone":  aws.StringValue(instance.Placement.AvailabilityZone),
		"ImageID":           aws.StringValue(instance.ImageId),
		"SubnetID":          aws.StringValue(instance.SubnetId),
		"KernelId":          aws.StringValue(instance.KernelId),
		"State":             aws.StringValue(instance.State.Name),
		"Platform":          aws.StringValue(instance.PlatformDetails),
		"InstanceLifecycle": aws.StringValue(instance.InstanceLifecycle),
		"Monitoring":        aws.StringValue(instance.Monitoring.State),
		// Add more metadata fields as needed
	}

	return data, nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <cloud provider (aws)> <instance Name>")
		return
	}
	instanceName := os.Args[2]

	aClient := InitAwsSession()
	metadata, err := aClient.GetAWSMetadata(instanceName)
	if err != nil {
		fmt.Println("Error retrieving metadata:", err)
		return
	}

	jsonData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling metadata to JSON:", err)
		return
	}

	fmt.Println(string(jsonData))
}
