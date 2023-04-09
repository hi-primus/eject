import { prepareColumns } from '../src/loadData';
import { describe, expect, test, it } from "vitest";

describe('prepareColumns', () => {
    it('returns all columns with "*" parameter', async () => {
        const result = await prepareColumns('*');
        expect(result).toEqual(['name', 'age', 'address', 'city']);
    });

    it('returns the name of a column with a number parameter', async () => {
        const result = await prepareColumns(2);
        expect(result).toEqual(['address']);
    });

    it('returns the names of columns with an array of numbers parameter', async () => {
        const result = await prepareColumns([0, 2]);
        expect(result).toEqual(['name', 'address']);
    });

    it('returns the names of columns that match a regular expression parameter', async () => {
        const result = await prepareColumns(/a/);
        expect(result).toEqual(['name', 'address', 'city']);
    });

    it('returns the input array parameter as is if it is an array of strings', async () => {
        const result = await prepareColumns(['name', 'address']);
        expect(result).toEqual(['name', 'address']);
    });

    it('returns an array of names that matches the input regex parameter in an array', async () => {
        const result = await prepareColumns([/a/, /e/]);
        expect(result).toEqual(['name', 'address', 'age']);
    });

    it('returns the name of a column if a single string parameter is given', async () => {
        const result = await prepareColumns('name');
        expect(result).toEqual(['name']);
    });

    it('throws an error for an invalid parameter type', async () => {
        try {
            await prepareColumns(123);
        } catch (error) {
            expect(error.message).toBe('Invalid input type');
        }
    });

    it('throws an error for an invalid column index', async () => {
        try {
            await prepareColumns(10);
        } catch (error) {
            expect(error.message).toBe('Invalid column index');
        }
    });

    it('filters the columns by data type', async () => {
        const result = await prepareColumns('*', false, false, 'string');
        expect(result).toEqual(['name', 'address', 'city']);
    });

    it('filters the columns by multiple data types', async () => {
        const result = await prepareColumns('*', false, false, ['number', 'string']);
        expect(result).toEqual(['name', 'age', 'address', 'city']);
    });

    it('inverts the selection if invert parameter is true', async () => {
        const result = await prepareColumns(['name', 'address'], true);
        expect(result).toEqual(['age', 'city']);
    });
});
