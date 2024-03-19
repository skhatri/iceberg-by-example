import org.w3c.dom.Element

plugins {
    id("java")
    id("idea")
    id("scala")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}


idea.project.ipr {
    withXml(Action<XmlProvider> {
        fun Element.firstElement(predicate: (Element.() -> Boolean)) =
            childNodes
                .run { (0 until length).map(::item) }
                .filterIsInstance<Element>()
                .first { it.predicate() }

        asElement()
            .firstElement { tagName == "component" && getAttribute("name") == "VcsDirectoryMappings" }
            .firstElement { tagName == "mapping" }
            .setAttribute("vcs", "Git")
    })
}

dependencies {
    implementation("org.scala-lang:scala-compiler:2.13.13")
    implementation("org.scala-lang:scala-library:2.13.13")

    listOf("spark-core_2.13", "spark-sql_2.13").forEach {
        implementation("org.apache.spark:${it}:3.5.1") {
            exclude(module="log4j-slf4j2-impl")
        }
    }

    listOf("iceberg-spark-3.5_2.13", "iceberg-spark-extensions-3.5_2.13").forEach {
        implementation("org.apache.iceberg:${it}:1.5.0")
    }
    implementation("org.apache.hadoop:hadoop-aws:3.3.6")

    implementation("ch.qos.logback:logback-classic:1.5.3")
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")
    implementation("org.xerial:sqlite-jdbc:3.45.1.0")
}

val targetApp = project.ext["main-class"]

val appArgs = "${project.ext["app-args"]}".split(" ").filter { x -> x != "" }

task("runTask", JavaExec::class) {
    mainClass = "${targetApp}"
    classpath = sourceSets["main"].runtimeClasspath
    jvmArgs = listOf(
        "-Xms512m", "-Xmx1024m", "-XX:+UseZGC",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED"
    )
    args = appArgs
}

repositories {
    mavenCentral()
}


