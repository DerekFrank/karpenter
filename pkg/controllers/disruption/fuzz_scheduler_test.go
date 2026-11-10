/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// The validation fuzz harness's ground truth is the real kube-scheduler, run against the suite's envtest apiserver.
// Pods the harness builds use fuzzParkedScheduler, which nothing serves, so kube-scheduler only places the pods the
// ground truth hands it under its default name.
const (
	fuzzParkedScheduler  = "fuzz-parked"
	fuzzDefaultScheduler = "default-scheduler"
)

var (
	kubeSchedulerOnce sync.Once
	kubeSchedulerErr  error
	kubeScheduler     *exec.Cmd
)

// stopKubeScheduler kills kube-scheduler, if it started, so the apiserver can stop.
func stopKubeScheduler() {
	if kubeScheduler != nil && kubeScheduler.Process != nil {
		_ = kubeScheduler.Process.Kill()
		_, _ = kubeScheduler.Process.Wait()
	}
}

// startKubeScheduler starts KUBE_SCHEDULER_BINARY against the suite's apiserver, once per Ginkgo process. It dies
// with the test process.
func startKubeScheduler(dir string) error {
	kubeSchedulerOnce.Do(func() {
		binary := os.Getenv("KUBE_SCHEDULER_BINARY")
		if binary == "" {
			kubeSchedulerErr = fmt.Errorf("KUBE_SCHEDULER_BINARY is unset")
			return
		}
		cfg := env.Config
		kubeconfig := clientcmdapi.NewConfig()
		kubeconfig.Clusters["envtest"] = &clientcmdapi.Cluster{Server: cfg.Host, CertificateAuthorityData: cfg.CAData}
		kubeconfig.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData, Token: cfg.BearerToken}
		kubeconfig.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "envtest"}
		kubeconfig.CurrentContext = "envtest"
		path := filepath.Join(dir, fmt.Sprintf("kube-scheduler-%d.kubeconfig", GinkgoParallelProcess()))
		if kubeSchedulerErr = clientcmd.WriteToFile(*kubeconfig, path); kubeSchedulerErr != nil {
			return
		}
		// Retry unschedulable pods within a second, so the ground truth settles soon after the cluster does.
		config := filepath.Join(dir, fmt.Sprintf("kube-scheduler-%d.yaml", GinkgoParallelProcess()))
		if kubeSchedulerErr = os.WriteFile(config, []byte(fmt.Sprintf(`apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: %s
leaderElection:
  leaderElect: false
podInitialBackoffSeconds: 1
podMaxBackoffSeconds: 1
%s`, path, fuzzSchedulerProfiles())), 0o600); kubeSchedulerErr != nil {
			return
		}
		cmd := exec.Command(binary, "--config="+config, "--secure-port=0", "-v=2") //nolint:gosec
		log, err := os.Create(filepath.Join(dir, fmt.Sprintf("kube-scheduler-%d.log", GinkgoParallelProcess())))
		if err != nil {
			kubeSchedulerErr = err
			return
		}
		cmd.Stdout, cmd.Stderr = log, log
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
		kubeSchedulerErr = cmd.Start()
		kubeScheduler = cmd
	})
	return kubeSchedulerErr
}

// fuzzSchedulerProfiles sets kube-scheduler's NodeResourcesFit scoring strategy to FUZZ_SCORING, such as MostAllocated,
// in place of its default LeastAllocated.
func fuzzSchedulerProfiles() string {
	strategy := os.Getenv("FUZZ_SCORING")
	if strategy == "" {
		return ""
	}
	return fmt.Sprintf(`profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: NodeResourcesFit
    args:
      scoringStrategy:
        type: %s
        resources:
        - name: cpu
          weight: 1
        - name: memory
          weight: 1
`, strategy)
}
