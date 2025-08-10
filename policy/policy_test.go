package policy

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type policyTestSuite struct {
	suite.Suite
	policy Policy
}

func (suite *policyTestSuite) SetupSuite() {
	path := filepath.Join(".", "permissions.json")

	ctx := context.Background()
	p, err := NewRegoPolicy(ctx, path)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.policy = p
}

func (suite *policyTestSuite) TestEvalListUsersWithAdminRole() {
	input := map[string]any{
		"domain": "identity::users",
		"action": "list",
		"claims": map[string]any{
			"roles": []string{"admin"},
		},
	}

	ctx := context.Background()
	accepted, err := suite.policy.Eval(ctx, input)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.True(accepted)
}

func (suite *policyTestSuite) TestEvalNotListUsersWithUserRole() {
	input := map[string]any{
		"domain": "identity::users",
		"action": "list",
		"claims": map[string]any{
			"roles": []string{"user"},
		},
	}

	ctx := context.Background()
	accepted, err := suite.policy.Eval(ctx, input)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.False(accepted)
}

func (suite *policyTestSuite) TestEvalUpdateUsersWithUserRoleAndOwner() {
	input := map[string]any{
		"domain":    "identity::users",
		"action":    "update",
		"object":    "mirror520",
		"who_flags": 0b0001,
		"claims": map[string]any{
			"sub":   "mirror520",
			"roles": []string{"user"},
		},
	}

	ctx := context.Background()
	accepted, err := suite.policy.Eval(ctx, input)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.True(accepted)
}

func (suite *policyTestSuite) TestEvalUpdateUsersWithUserRoleAndNotOwner() {
	input := map[string]any{
		"domain":    "identity::users",
		"action":    "update",
		"object":    "mirror",
		"who_flags": 0b0001,
		"claims": map[string]any{
			"sub":   "mirror520",
			"roles": []string{"user"},
		},
	}

	ctx := context.Background()
	accepted, err := suite.policy.Eval(ctx, input)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.False(accepted)
}

func (suite *policyTestSuite) TestEvalUpdateUsersWithAdminRoleAndAdmin() {
	input := map[string]any{
		"domain":    "identity::users",
		"action":    "update",
		"who_flags": 0b1000,
		"claims": map[string]any{
			"sub":   "mirror520",
			"roles": []string{"admin"},
		},
	}

	ctx := context.Background()
	accepted, err := suite.policy.Eval(ctx, input)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.True(accepted)
}

func (suite *policyTestSuite) TestEvalUpdateUsersWithUserRoleAndNotAdmin() {
	input := map[string]any{
		"domain":    "identity::users",
		"action":    "update",
		"who_flags": 0b1000,
		"claims": map[string]any{
			"sub":   "mirror520",
			"roles": []string{"user"},
		},
	}

	ctx := context.Background()
	accepted, err := suite.policy.Eval(ctx, input)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.False(accepted)
}

func (suite *policyTestSuite) TestEvalWithNoAuthorizedUsers() {
	input := map[string]any{
		"domain":    "identity::users",
		"action":    "update",
		"who_flags": 0b0000, // 沒有設置任何標誌
		"claims": map[string]any{
			"roles": []string{"user"},
		},
	}

	ctx := context.Background()
	accepted, err := suite.policy.Eval(ctx, input)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.True(accepted)
}

func TestPolicyTestSuite(t *testing.T) {
	suite.Run(t, new(policyTestSuite))
}
