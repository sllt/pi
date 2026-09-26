package main

import (
	"context"
	"fmt"
	"os"

	"github.com/sllt/pi/pkg/pi/cli/bootstrap"
	"github.com/sllt/pi/pkg/pi/cli/create"
	"github.com/sllt/pi/pkg/pi/cli/migration"
	"github.com/sllt/pi/pkg/pi/cli/wrap"
	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:    "pi",
		Usage:   "Pi Framework CLI - Code generation and project management tool",
		Version: CLIVersion,
		Commands: []*cli.Command{
			{
				Name:  "init",
				Usage: "Initialize a new Pi project",
				Arguments: []cli.Argument{
					&cli.StringArg{
						Name: "project-name",
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					name := cmd.StringArg("project-name")
					if name == "" {
						return fmt.Errorf("please provide a project name, e.g.: pi init myproject")
					}
					return bootstrap.Create(name)
				},
			},
			{
				Name:  "create",
				Usage: "Create layered architecture components",
				Commands: []*cli.Command{
					newCreateCommand("handler", "Create a new handler"),
					newCreateCommand("service", "Create a new service"),
					newCreateCommand("repository", "Create a new repository"),
					newCreateCommand("model", "Create a new model"),
					{
						Name:  "all",
						Usage: "Create handler, service, repository, and model",
						Arguments: []cli.Argument{
							&cli.StringArg{
								Name: "name",
							},
						},
						Action: func(ctx context.Context, cmd *cli.Command) error {
							name := cmd.StringArg("name")
							if name == "" {
								return fmt.Errorf("please provide a name, e.g.: pi create all user")
							}
							result, err := create.All(name)
							if err != nil {
								return err
							}
							fmt.Println(result)
							return nil
						},
					},
				},
			},
			{
				Name:  "migrate",
				Usage: "Database migration tools",
				Commands: []*cli.Command{
					{
						Name:  "create",
						Usage: "Create a new migration file",
						Arguments: []cli.Argument{
							&cli.StringArg{
								Name: "migration-name",
							},
						},
						Action: func(ctx context.Context, cmd *cli.Command) error {
							name := cmd.StringArg("migration-name")
							if name == "" {
								return fmt.Errorf("please provide a migration name, e.g.: pi migrate create add_users")
							}
							result, err := migration.Migrate(name)
							if err != nil {
								return err
							}
							fmt.Println(result)
							return nil
						},
					},
				},
			},
			{
				Name:  "wrap",
				Usage: "Generate Pi-integrated wrapper code",
				Commands: []*cli.Command{
					{
						Name:  "grpc",
						Usage: "Generate gRPC wrapper code",
						Commands: []*cli.Command{
							{
								Name:  "server",
								Usage: "Generate Pi-integrated gRPC server from proto file",
								Flags: []cli.Flag{
									&cli.StringFlag{
										Name:     "proto",
										Usage:    "Path to the proto file",
										Required: true,
									},
									&cli.StringFlag{
										Name:  "out",
										Usage: "Output directory (default: same as proto file)",
									},
								},
								Action: func(ctx context.Context, cmd *cli.Command) error {
									protoPath := cmd.String("proto")
									outDir := cmd.String("out")
									result, err := wrap.BuildGRPCPiServer(protoPath, outDir)
									if err != nil {
										return err
									}
									fmt.Println(result)
									return nil
								},
							},
							{
								Name:  "client",
								Usage: "Generate Pi-integrated gRPC client from proto file",
								Flags: []cli.Flag{
									&cli.StringFlag{
										Name:     "proto",
										Usage:    "Path to the proto file",
										Required: true,
									},
									&cli.StringFlag{
										Name:  "out",
										Usage: "Output directory (default: same as proto file)",
									},
								},
								Action: func(ctx context.Context, cmd *cli.Command) error {
									protoPath := cmd.String("proto")
									outDir := cmd.String("out")
									result, err := wrap.BuildGRPCPiClient(protoPath, outDir)
									if err != nil {
										return err
									}
									fmt.Println(result)
									return nil
								},
							},
						},
					},
				},
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// newCreateCommand creates a subcommand for "pi create <type> <name>".
func newCreateCommand(createType, usage string) *cli.Command {
	return &cli.Command{
		Name:  createType,
		Usage: usage,
		Arguments: []cli.Argument{
			&cli.StringArg{
				Name: "name",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			name := cmd.StringArg("name")
			if name == "" {
				return fmt.Errorf("please provide a name, e.g.: pi create %s user", createType)
			}
			result, err := create.CreateComponent(name, createType)
			if err != nil {
				return err
			}
			fmt.Println(result)
			return nil
		},
	}
}
