Publishing a new release
========================

One-off setup:

1. [Create an account](https://issues.sonatype.org/secure/Signup!default.jspa) on Sonatype's JIRA.
2. Submit a ticket to [request access](http://central.sonatype.org/pages/ossrh-guide.html).
   This project's repository was created in [OSSRH-9734](https://issues.sonatype.org/browse/OSSRH-9734).
3. Log in to [Nexus](https://oss.sonatype.org/) using the JIRA credentials, click the 'Profile' menu
   item to [request a user token](https://oss.sonatype.org/#profile;User%20Token).
4. Edit `$HOME/.gradle/gradle.properties`, set `nexusUsername=...` to the user token username, and set
   `nexusPassword=...` to the user token password.
5. Make sure you have a GnuPG private key set up. Edit `$HOME/.gradle/gradle.properties` and set
   `signing.keyId=...` to the key ID you want to use for signing (an 8-digit hex string).

For each release:

 1. Set the version number in `gradle.properties`.
 2. `./gradlew clean uploadArchives`
     *  If the version number ends in `-SNAPSHOT`, this will skip signing, and upload the built jar
        files to the [Sonatype OSS snapshot repository](https://oss.sonatype.org/content/repositories/snapshots/).
     *  Otherwise, this will ask for your GnuPG passphrase, build and sign the artifacts, and upload them
        to the [Sonatype OSS staging repository](https://oss.sonatype.org/content/groups/staging/).
 3. Log in to [OSSRH Nexus](https://oss.sonatype.org/), choose *Staging Repositories* from the left-hand menu,
    select the appropriate repository and click "Close". When this has completed, select the repository again
    and click "Release". ([Docs](http://central.sonatype.org/pages/releasing-the-deployment.html))
 4. Update README.md with the latest released version number.
 5. The javadocs are under `build/docs/javadoc`. Check out the `gh-pages` branch and publish the javadocs
    under the current version number.
