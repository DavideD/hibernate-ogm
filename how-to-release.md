# Release Hiberante OGM

## Requirements

Make sure you have:

1. **JDK 7** for the build (the created artifacts are compatible with Java 6)

2. **Maven** configured to use the JBoss repositories, with permissions to upload. Make sure your settings.xml is configured accordingly or use the option -s settings-example.xml when running the commands.

3. all the permissions required to upload the packages on:

  - [Nexus](https://repository.jboss.org/nexus/index.html): you can try to login on the Nexus web interface
  - [SourceForge](https://sourceforge.net): you need to have the authorization on the hibernate-ogm project
  - Documentation: you have to be able to connect via ssh to hibernate@filemgmt.jboss.org:/docs_htdocs/hibernate/ogm/[version]

## Release process

To prepare and release a new version of Hibernate OGM follow these steps (executed against the branch you intend to release):

### Preparation

Verify:

1. the project status on [Jenkins](http://ci.hibernate.org/view/OGM/)

2. there are no outstanding issues in JIRA

3. tests and artifacts:

   ```
       mvn clean install -Pdist -s settings-example.xml 
   ```

4. the distribution package as built by Maven (distribution/target/hibernate-ogm-[version]-dist).

   They should contain the appropriate dependencies, without duplicates. The creation of these directories is driven by the assembly plugin (distribution/src/main/assembly/dist.xml) which is very specific and might break with the inclusion of new dependencies.

   Especially check the jar files in the subdirectories:
   - optional
   - required
   - provided

### Release

1. [Release the version on JIRA](https://hibernate.atlassian.net/plugins/servlet/project-config/OGM/versions)

2. Update the _changelog.txt_ in project root from [JIRA's release notes](https://hibernate.atlassian.net/secure/ReleaseNote.jspa?projectId=10061)

3. Verify _readme.txt_:
   - content is up to date
   - links are not broken
   - current date and version number in the header

4. Commit any outstanding changes

5. Tag and build the release using the [maven release plugin](http://maven.apache.org/plugins/maven-release-plugin)

   During release:prepare you will have to specify the tag name and release version

   ```
       mvn release:prepare
       mvn release:perform   
       git push upstream HEAD  
       git push upstream <release-version>  
   ```

6. Log in to [Nexus](https://repository.jboss.org/nexus):
   - check all artifacts you expect are there
   - close and release the repository. See [more details about using the staging repository](https://community.jboss.org/wiki/MavenDeployingARelease)
   - if there is a problem, drop the staging repo, fix the problem and re-deploy

7. If it's a final release, create maintenance branches for the previous version.

   ```
       mvn versions:set -DnewVersion=A.B.C-SNAPSHOT
   ```

   git add, commit, push upstream the new branch.

### Publish

1. Upload the distribution packages to SourceForge (they should be under target/checkout/target). You need to be member of the Hibernate project team of Sourceforge to do that. [See Sourceforge instructions](https://sourceforge.net/p/forge/documentation/Release%20Files%20for%20Download/)
   - Copy the _changelog.txt__ (in target/checkout/distribution/target/hibernate-ogm-[version]-dist)
   - Copy the _readme.txt__ (in target/checkout/distribution/target/hibernate-ogm-[version]-dist)
   - Copy the _.zip distribution_ (in target/checkout/distribution/target)
   - Copy the _.tar.gz distribution_ (in target/checkout/distribution/target)
   - Copy the .zip containing the _JBoss Modules_ (in target/checkout/modules/target)

2. Upload the documentation to docs.jboss.org. You should be able to use rsync to get the documentation via (provided you are in the docs directory of the unpacked distribution):

   ```
       rsync -rzh --progress --delete \
             --protocol=28 docs/ hibernate@filemgmt.jboss.org:/docs_htdocs/hibernate/ogm/<version-family>
   ```

   or alternatively

   ```
       scp -r api hibernate@filemgmt.jboss.org:docs_htdocs/hibernate/ogm/<version-family>
       scp -r reference hibernate@filemgmt.jboss.org:docs_htdocs/hibernate/ogm/<version-family>
   ```

3. If it is a final release you have to add the symbolic link /docs_htdocs/hibernate/stable/ogm
   You can't create symlinks on the server so you either create it locally then rsync it up, or make a copy of the documentation in that URL.

### Announce

1. Update the community pages. Check:     
  - http://community.jboss.org/en/hibernate/ogm

2. Blog about the release on _in.relation.to_, make sure to use the tags **Hibernate OGM**, **Hibernate** and **news** for the blog entry.
   This way the blog will be featured on http://www.hibernate.org/subprojects/ogm) and on the JBoss blog federation.

3. Update _hibernate.org_ by adding a new release file to **_data/projects/ogm/releases** and deploying to production (you might want to delete an older release file, if you don't want it displayed anymore)

   Check:
   - http://www.hibernate.org/subprojects/ogm/download
   - http://www.hibernate.org/subprojects/ogm/docs
 
4. Send email to __hibernate-dev__ and __hibernate-announce__

5. __Twitter__
