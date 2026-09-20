package main

import (
	"os"

	"github.com/kevinmingtarja/scheduler-plugins/pkg/maint"
	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	_ "k8s.io/component-base/metrics/prometheus/version"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	command := app.NewSchedulerCommand(app.WithPlugin(maint.Name, maint.New))
	os.Exit(cli.Run(command))
}
