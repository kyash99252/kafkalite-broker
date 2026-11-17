package com.kafkalite.core;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(
        packages = "com.kafkalite",
        importOptions = ImportOption.DoNotIncludeTests.class
)
public class ArchitectureTest {

    @ArchTest
    static final ArchRule storageMustNotDependOnNetworkOrGroup =
            noClasses()
                    .that()
                    .resideInAPackage("..core.storage..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage("..core.network..", "..core.group..");

    @ArchTest
    static final ArchRule protocolMustBeIndependent =
            noClasses()
                    .that()
                    .resideInAPackage("..protocol..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage("..core..", "..client..");

    @ArchTest
    static final ArchRule productionCodeMustNotUseSystemOut =
            noClasses()
                    .that()
                    .resideOutsideOfPackage("..lifecycle..")
                    .should()
                    .accessClassesThat()
                    .haveFullyQualifiedName(System.class.getName())
                    .because("Use SLF4J logging instead of System.out/err in broker internals");
}
