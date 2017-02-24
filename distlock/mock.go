package distlock

type MockReleaser struct {
}

func (mr *MockReleaser) Release() error {
	return nil
}

type MockLocker struct {
}

func (ml *MockLocker) Lock(string) (Releaser, bool, error) {
	return &MockReleaser{}, true, nil
}

func (ml *MockLocker) LockWait(string) (Releaser, error) {
	return &MockReleaser{}, nil
}
