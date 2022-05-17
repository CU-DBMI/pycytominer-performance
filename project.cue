package main

import (
	"dagger.io/dagger"
	"universe.dagger.io/docker"
)

dagger.#Plan & {

	client: {
		filesystem: {
			"./": read: contents:             dagger.#FS
			"./src": write: contents:         actions.clean.black.export.directories."/workdir/src"
			"./project.cue": write: contents: actions.clean.cue.export.files."/workdir/project.cue"
			"./deployment": write: contents:  actions.clean.terraform.export.directories."/workdir/deployment"
		}

	}
	python_version: string | *"3.9"

	actions: {
		// build base python image for linting and testing
		_python_pre_build: docker.#Build & {
			steps: [
				docker.#Pull & {
					source: "python:" + python_version
				},
				docker.#Run & {
					command: {
						name: "mkdir"
						args: ["/workdir"]
					}
				},

				docker.#Run & {
					command: {
						name: "apt"
						args: ["update"]
					}
				},
				docker.#Run & {
					command: {
						name: "apt"
						args: ["install", "--no-install-recommends", "-y",
							"build-essential", "libunwind-dev", "liblz4-dev"]
					}
				},
				docker.#Run & {
					command: {
						name: "apt"
						args: ["clean"]
					}
				},
				docker.#Run & {
					command: {
						name: "rm"
						args: ["-rf", "/var/lib/apt/lists/*"]
					}
				},
				docker.#Copy & {
					contents: client.filesystem."./".read.contents
					source:   "./requirements.txt"
					dest:     "/workdir/requirements.txt"
				},
				docker.#Run & {
					workdir: "/workdir"
					command: {
						name: "pip"
						args: ["install", "--no-cache-dir", "-r", "requirements.txt"]
					}
				},
			]
		}
		_python_build: docker.#Build & {
			steps: [
				docker.#Copy & {
					input:    _python_pre_build.output
					contents: client.filesystem."./".read.contents
					source:   "./"
					dest:     "/workdir"
				},
			]
		}
		// cuelang build
		_cue_pre_build: docker.#Build & {
			steps: [
				docker.#Pull & {
					source: "golang:latest"
				},
				docker.#Run & {
					command: {
						name: "mkdir"
						args: ["/workdir"]
					}
				},

				docker.#Run & {
					command: {
						name: "go"
						args: ["install", "cuelang.org/go/cmd/cue@latest"]
					}
				},
			]
		}
		_cue_build: docker.#Build & {
			steps: [
				docker.#Copy & {
					input:    _cue_pre_build.output
					contents: client.filesystem."./".read.contents
					source:   "./project.cue"
					dest:     "/workdir/project.cue"
				},
			]
		}

		// terraform build
		_tf_pre_build: docker.#Build & {
			steps: [
				docker.#Pull & {
					source: "hashicorp/terraform:latest"
				},
				docker.#Run & {
					entrypoint: ["pwd"]
					command: {
						name: "mkdir"
						args: ["/workdir"]
					}
				},
			]
		}
		_tf_build: docker.#Build & {
			steps: [
				docker.#Copy & {
					input:    _tf_pre_build.output
					contents: client.filesystem."./".read.contents
					source:   "./deployment"
					dest:     "/workdir/deployment"
				},
			]
		}

		// applied code and/or file formatting
		clean: {
			// sort python imports with isort
			isort: docker.#Run & {
				input:   _python_build.output
				workdir: "/workdir"
				command: {
					name: "python"
					args: ["-m", "isort", "--profile", "black", "src/"]
				}
			}
			// code style formatting with black
			black: docker.#Run & {
				input:   isort.output
				workdir: "/workdir"
				command: {
					name: "python"
					args: ["-m", "black", "src/"]
				}
				export: {
					directories: {
						"/workdir/src": _
					}
				}
			}
			// code formatting for cuelang
			cue: docker.#Run & {
				input:   _cue_build.output
				workdir: "/workdir"
				command: {
					name: "cue"
					args: ["fmt", "/workdir/project.cue"]
				}
				export: {
					files: "/workdir/project.cue": _
				}
			}
			// code formatting for terraform
			terraform: docker.#Run & {
				input:   _tf_build.output
				workdir: "/workdir"
				command: {
					name: "fmt"
					args: ["-recursive", "/workdir/"]
				}
				export: {
					directories: "/workdir/deployment": _
				}

			}
		}
		// linting for formatting and best practices
		lint: {
			// isort (imports) formatting check
			isort: docker.#Run & {
				input:   _python_build.output
				workdir: "/workdir"
				command: {
					name: "python"
					args: ["-m", "isort", "--profile", "black", "--check", "--diff", "src/"]
				}
			}
			// black formatting check
			black: docker.#Run & {
				input:   isort.output
				workdir: "/workdir"
				command: {
					name: "python"
					args: ["-m", "black", "--check", "src/"]
				}
			}
			// safety security vulnerabilities check
			safety: docker.#Run & {
				input:   black.output
				workdir: "/workdir"
				command: {
					name: "python"
					args: ["-m", "safety", "check"]
				}
			}
			// bandit security vulnerabilities check
			bandit: docker.#Run & {
				input:   safety.output
				workdir: "/workdir"
				command: {
					name: "python"
					args: ["-m", "bandit", "-r", "src/"]
				}
			}
		}
	}
}
