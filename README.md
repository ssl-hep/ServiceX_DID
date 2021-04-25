# ServiceX_DID

 ServiceX DID Library

## Left to do

- Coverage/CI in github
- Automatic push to pypi
- look for all post_status_update calls in the original
- In general - look for all calls back to the servicex object!

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
```
