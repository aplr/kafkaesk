<?php

namespace GrahamCampbell\Tests\Markdown;

use PHPUnit\Framework\TestCase;
use GrahamCampbell\Analyzer\AnalysisTrait;

/**
 * This is the analysis test class.
 *
 * @author Graham Campbell <graham@alt-three.com>
 */
class AnalysisTest extends TestCase
{
    use AnalysisTrait;

    /**
     * Get the code paths to analyze.
     *
     * @return string[]
     */
    protected function getPaths()
    {
        return [
            realpath(__DIR__ . '/../config'),
            realpath(__DIR__ . '/../src'),
            realpath(__DIR__),
        ];
    }
}
