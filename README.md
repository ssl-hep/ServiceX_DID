# ServiceX_DID

 ServiceX DID Library

## Left to do

- In general - look for all calls back to the servicex object!
- Write docs in here on how to use this
- Automatic push to pypi

## Notes to keep for a short while

Add the following back into the ci for publishing

```yaml
  publish:
    needs: test
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@master

    - name: Extract tag name
      shell: bash
      run: echo "##[set-output name=imagetag;]$(echo ${GITHUB_REF##*/})"
      id: extract_tag_name

    - name: Build DID-Finder Image
      uses: elgohr/Publish-Docker-Github-Action@master
      with:
        name: sslhep/servicex-did-finder:${{ steps.extract_tag_name.outputs.imagetag }}
        username: ${{ secrets.DOCKER_USERNAME }}
        password: ${{ secrets.DOCKER_PASSWORD }}
        tag: "${GITHUB_REF##*/}"


      run: |
        poetry run coverage report
        # codecov --token=${{ secrets.CODECOV_TOKEN }}

```
