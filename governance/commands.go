package governance

import (
	"fmt"
	"github.com/open-horizon/anax/persistence"
)

type StartGovernExecutionCommand struct {
	AgreementId       string
	AgreementProtocol string
	Deployment        map[string]persistence.ServiceConfig
}

func (g StartGovernExecutionCommand) ShortString() string {
	depStr := ""
	for key, _ := range g.Deployment {
		depStr = depStr + key + ","
	}

	return fmt.Sprintf("GovernExecutionCommand: AgreementId %v, AgreementProtocol %v, Deployed Services %v", g.AgreementId, g.AgreementProtocol, depStr)
}

func (w *GovernanceWorker) NewStartGovernExecutionCommand(deployment map[string]persistence.ServiceConfig, protocol string, agreementId string) *StartGovernExecutionCommand {
	return &StartGovernExecutionCommand{
		AgreementId:       agreementId,
		AgreementProtocol: protocol,
		Deployment:        deployment,
	}
}

// ==============================================================================================================
type CleanupExecutionCommand struct {
	AgreementProtocol string
	AgreementId       string
	Reason            uint
	Deployment        map[string]persistence.ServiceConfig
}

func (c CleanupExecutionCommand) ShortString() string {
	depStr := ""
	for key, _ := range c.Deployment {
		depStr = depStr + key + ","
	}

	return fmt.Sprintf("CleanupExecutionCommand: AgreementId %v, AgreementProtocol %v, Reason %v, Deployed Services %v", c.AgreementId, c.AgreementProtocol, c.Reason, depStr)
}

func (w *GovernanceWorker) NewCleanupExecutionCommand(protocol string, agreementId string, reason uint, deployment map[string]persistence.ServiceConfig) *CleanupExecutionCommand {
	return &CleanupExecutionCommand{
		AgreementProtocol: protocol,
		AgreementId:       agreementId,
		Reason:            reason,
		Deployment:        deployment,
	}
}

// ==============================================================================================================
type CleanupStatusCommand struct {
	AgreementProtocol string
	AgreementId       string
	Status            uint
}

func (c CleanupStatusCommand) ShortString() string {

	return fmt.Sprintf("CleanupStatusCommand: AgreementId %v, AgreementProtocol %v, Status %v", c.AgreementId, c.AgreementProtocol, c.Status)
}

func (w *GovernanceWorker) NewCleanupStatusCommand(protocol string, agreementId string, status uint) *CleanupStatusCommand {
	return &CleanupStatusCommand{
		AgreementProtocol: protocol,
		AgreementId:       agreementId,
		Status:            status,
	}
}

// ==============================================================================================================
type AsyncTerminationCommand struct {
	AgreementId       string
	AgreementProtocol string
	Reason            uint
}

func (c AsyncTerminationCommand) ShortString() string {

	return fmt.Sprintf("AsyncTerminationCommand: AgreementId %v, AgreementProtocol %v, Reason %v", c.AgreementId, c.AgreementProtocol, c.Reason)
}

func NewAsyncTerminationCommand(agreementId string, agreementProtocol string, reason uint) *AsyncTerminationCommand {
	return &AsyncTerminationCommand{
		AgreementId:       agreementId,
		AgreementProtocol: agreementProtocol,
		Reason:            reason,
	}
}

// ==============================================================================================================
type StartMicroserviceCommand struct {
	MsDefKey string
}

func (c StartMicroserviceCommand) ShortString() string {
	return fmt.Sprintf("StartMicroserviceCommand: MsDefKey %v", c.MsDefKey)
}

func (w *GovernanceWorker) NewStartMicroserviceCommand(key string) *StartMicroserviceCommand {
	return &StartMicroserviceCommand{
		MsDefKey: key,
	}
}

// ==============================================================================================================
type UpdateMicroserviceInstanceCommand struct {
	MsInstKey            string // the name that was passed into the ContainerLaunchContext, it is the key to the MicroserviceInstance table.
	ExecutionStarted     bool
	ExecutionFailureCode uint
	ExecutionFailureDesc string
}

func (c UpdateMicroserviceInstanceCommand) ShortString() string {
	return fmt.Sprintf("UpdateMicroserviceInstanceCommand: MsInstKey %v, ExecutionStarted %v, ExecutionFailureCode %v, ExecutionFailureDesc %v",
		c.MsInstKey, c.ExecutionStarted, c.ExecutionFailureCode, c.ExecutionFailureDesc)
}

func (w *GovernanceWorker) NewUpdateMicroserviceInstanceCommand(key string, started bool, failure_code uint, failure_desc string) *UpdateMicroserviceInstanceCommand {
	return &UpdateMicroserviceInstanceCommand{
		MsInstKey:            key,
		ExecutionStarted:     started,
		ExecutionFailureCode: failure_code,
		ExecutionFailureDesc: failure_desc,
	}
}
